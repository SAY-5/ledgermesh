package io.ledgermesh.payment.authorize;

import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import io.ledgermesh.payment.domain.Payment;
import io.ledgermesh.payment.domain.PaymentRepository;
import io.ledgermesh.payment.domain.PaymentStatus;
import io.ledgermesh.payment.processor.PaymentProcessor;
import io.ledgermesh.payment.processor.PaymentProcessor.Authorization;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Enforces from the processor's side that the card holds only what a payment keeps. On a schedule,
 * it lists the merchant's outstanding authorizations and releases every one older than the grace
 * period that its payment does not keep: all of them when the payment is voided, declined or
 * absent, every other code when it is authorized, and, while it is still open, every one it has not
 * committed, since an approval whose commit failed is never kept and the payment will be attempted
 * again. A payment keeps only the code it is authorized under, which is never released.
 *
 * <p>The grace period is the longest a processor call can run, taken from the processor and the
 * configured time limits, plus {@code ledgermesh.payment.reconcile.commit-allowance} for the answer
 * to be committed. An authorization younger than that may be on its way to becoming the kept one
 * and is left alone; one older than that is past its commit, or never going to have one. So no
 * authorization a payment does not keep survives more than the grace period plus one interval
 * ({@code ledgermesh.payment.reconcile-ms}), whatever became of the answer that granted it. The
 * release path of voids and late approvals is quicker for the cases it sees; this is what holds
 * when an answer's commit is lost to a race, a rollback or a killed process.
 */
@Component
@ConditionalOnProperty(
    name = "ledgermesh.payment.reconciler",
    havingValue = "true",
    matchIfMissing = true)
public class HoldReconciler {

  private static final Logger log = LoggerFactory.getLogger(HoldReconciler.class);

  private final PaymentProcessor processor;
  private final PaymentRepository payments;
  private final Clock clock;
  private final Duration grace;
  private final Duration interval;
  private final int page;
  private final Counter released;
  private final AtomicLong oldestUnkeptSeconds = new AtomicLong();

  public HoldReconciler(
      PaymentProcessor processor,
      PaymentRepository payments,
      TimeLimiterRegistry limiters,
      Clock clock,
      MeterRegistry meters,
      @Value("${ledgermesh.payment.reconcile.commit-allowance:5s}") Duration commitAllowance,
      @Value("${ledgermesh.payment.reconcile-ms:5000}") long intervalMillis,
      @Value("${ledgermesh.payment.reconcile.page:1000}") int page) {
    this.processor = processor;
    this.payments = payments;
    this.clock = clock;
    Duration call =
        Stream.of(
                processor.longestCall(),
                limit(limiters, ResilientProcessorCall.RESILIENCE_NAME),
                limit(limiters, ResilientProcessorCall.DEFERRED_NAME))
            .filter(d -> d != null)
            .max(Duration::compareTo)
            .orElse(Duration.ZERO);
    this.grace = call.plus(commitAllowance);
    this.interval = Duration.ofMillis(intervalMillis);
    this.page = page;
    this.released = meters.counter("ledgermesh.payments.reconciled.released");
    meters.gauge("ledgermesh.payments.reconcile.grace.seconds", grace, d -> d.toMillis() / 1000.0);
    meters.gauge(
        "ledgermesh.payments.reconcile.interval.seconds", interval, d -> d.toMillis() / 1000.0);
    meters.gauge(
        "ledgermesh.payments.unkept.oldest.seconds", oldestUnkeptSeconds, AtomicLong::doubleValue);
    log.info("reconciling authorizations older than {} every {}", grace, interval);
  }

  private static Duration limit(TimeLimiterRegistry limiters, String name) {
    return limiters.timeLimiter(name).getTimeLimiterConfig().getTimeoutDuration();
  }

  public Duration grace() {
    return grace;
  }

  @Scheduled(
      initialDelayString = "${ledgermesh.payment.reconcile-ms:5000}",
      fixedDelayString = "${ledgermesh.payment.reconcile-ms:5000}")
  public void tick() {
    reconcile(clock.instant());
  }

  /**
   * Releases every outstanding authorization granted before {@code now} minus the grace period that
   * its payment does not keep, paging through the whole listing, and records the age of the oldest
   * outstanding authorization no payment keeps. Returns how many it released. Asking twice releases
   * nothing more.
   */
  public int reconcile(Instant now) {
    Instant cutoff = now.minus(grace);
    int count = 0;
    Instant oldest = null;
    Authorization after = null;
    while (true) {
      List<Authorization> listed = processor.outstanding(after, page);
      if (listed.isEmpty()) {
        break;
      }
      Pass pass = reconcile(listed, cutoff);
      count += pass.released();
      if (pass.oldest() != null && (oldest == null || pass.oldest().isBefore(oldest))) {
        oldest = pass.oldest();
      }
      if (listed.size() < page) {
        break;
      }
      after = listed.get(listed.size() - 1);
    }
    oldestUnkeptSeconds.set(oldest == null ? 0 : Duration.between(oldest, now).toSeconds());
    return count;
  }

  private record Pass(int released, Instant oldest) {}

  /** One page of the listing: releases what is past the cutoff and not kept. */
  private Pass reconcile(List<Authorization> outstanding, Instant cutoff) {
    Map<String, Payment> byOrder = new HashMap<>();
    for (Payment p :
        payments.findAllById(
            outstanding.stream().map(Authorization::orderId).distinct().toList())) {
      byOrder.put(p.getOrderId(), p);
    }
    int count = 0;
    Instant oldest = null;
    for (Authorization a : outstanding) {
      Payment payment = byOrder.get(a.orderId());
      if (keeps(payment, a.authorizationCode())) {
        continue;
      }
      if (a.grantedAt().isAfter(cutoff)) {
        oldest = oldest == null || a.grantedAt().isBefore(oldest) ? a.grantedAt() : oldest;
        continue;
      }
      try {
        if (processor.releaseAuthorization(a.orderId(), a.authorizationCode())) {
          count++;
          released.increment();
          log.warn(
              "released authorization {} of order {}, not kept by its {} payment",
              a.authorizationCode(),
              a.orderId(),
              payment == null ? "absent" : payment.getStatus());
        }
      } catch (RuntimeException e) {
        oldest = oldest == null || a.grantedAt().isBefore(oldest) ? a.grantedAt() : oldest;
        log.warn("release of {} failed, next pass: {}", a.authorizationCode(), e.toString());
      }
    }
    return new Pass(count, oldest);
  }

  /** A payment keeps only the code it is authorized under. */
  private static boolean keeps(Payment payment, String code) {
    return payment != null
        && payment.getStatus() == PaymentStatus.AUTHORIZED
        && code.equals(payment.getAuthorizationCode());
  }
}
