package io.ledgermesh.payment.processor;

import java.math.BigDecimal;

/**
 * The external card processor. Throws {@link ProcessorUnavailableException} on transient faults.
 */
public interface PaymentProcessor {

  sealed interface Result permits Approved, Declined {}

  record Approved(String authorizationCode) implements Result {}

  record Declined(String reason) implements Result {}

  /**
   * Asks for an authorization. Every approval is a new authorization with a code of its own, so two
   * approvals for one order, a timed out attempt and its retry say, are two holds on the card.
   */
  Result authorize(String orderId, String customerId, BigDecimal amount, int attempt);

  /**
   * Releases every authorization outstanding under the order's reference except the one whose code
   * is {@code keep} (none when {@code keep} is null), and returns how many it released. It goes by
   * the reference because the payment service does not hold the code of every authorization the
   * processor granted: one whose answer was lost when the payment service was killed before it
   * committed it is known only to the processor. Idempotent: once nothing but {@code keep} is
   * outstanding, a release releases nothing.
   */
  int release(String orderId, String keep);
}
