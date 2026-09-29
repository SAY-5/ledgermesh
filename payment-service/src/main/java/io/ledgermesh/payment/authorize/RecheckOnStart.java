package io.ledgermesh.payment.authorize;

import io.ledgermesh.payment.domain.PaymentRepository;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * A payment service that was killed may have had processor calls in flight, and an answer it never
 * received can still have left an authorization on the card, one the payment does not keep. On
 * start, every settled payment changed within {@code ledgermesh.payment.recheck-window} is asked
 * about again: the sweeper then has the processor release all but the kept authorization. The
 * window has to be longer than a restart plus the longest processor call.
 */
@Component
public class RecheckOnStart implements ApplicationRunner {

  private static final Logger log = LoggerFactory.getLogger(RecheckOnStart.class);

  private final PaymentRepository payments;
  private final Clock clock;
  private final Duration window;

  public RecheckOnStart(
      PaymentRepository payments,
      Clock clock,
      @Value("${ledgermesh.payment.recheck-window:10m}") Duration window) {
    this.payments = payments;
    this.clock = clock;
    this.window = window;
  }

  @Override
  public void run(ApplicationArguments args) {
    Instant now = clock.instant();
    int marked = payments.recheckSince(now.minus(window), now);
    if (marked > 0) {
      log.info(
          "{} payment(s) changed in the last {} will be checked with the processor",
          marked,
          window);
    }
  }
}
