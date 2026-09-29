package io.ledgermesh.payment.processor;

import java.math.BigDecimal;

/**
 * The external card processor. Throws {@link ProcessorUnavailableException} on transient faults.
 */
public interface PaymentProcessor {

  sealed interface Result permits Approved, Declined {}

  record Approved(String authorizationCode) implements Result {}

  record Declined(String reason) implements Result {}

  Result authorize(String orderId, String customerId, BigDecimal amount, int attempt);

  /**
   * Releases every authorization outstanding under the order's reference and returns how many it
   * released. It goes by the reference rather than by one authorization code because the payment
   * service does not always hold the code of what the processor granted: an approval whose answer
   * was cut off by the time limiter, or that landed while the payment service was being killed
   * before it could commit it, is still a live authorization on the card. Idempotent: an order with
   * nothing outstanding releases nothing, and a second release after the first does too.
   */
  int release(String orderId);
}
