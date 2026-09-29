package io.ledgermesh.payment.authorize;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Drains the deferred queue (payments the listener could not finish, or that were deferred) and
 * asks the processor again for every release of a voided payment it has not confirmed.
 */
@Component
@ConditionalOnProperty(
    name = "ledgermesh.payment.sweeper",
    havingValue = "true",
    matchIfMissing = true)
public class DeferredPaymentSweeper {

  private final PaymentService payments;

  public DeferredPaymentSweeper(PaymentService payments) {
    this.payments = payments;
  }

  @Scheduled(fixedDelayString = "${ledgermesh.payment.sweep-ms:1000}")
  public void tick() {
    payments.sweep();
    payments.sweepReleases();
  }
}
