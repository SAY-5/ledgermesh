package io.ledgermesh.payment.processor;

import java.math.BigDecimal;

/** The external card processor. Throws {@link ProcessorUnavailableException} on transient faults. */
public interface PaymentProcessor {

  sealed interface Result permits Approved, Declined {}

  record Approved(String authorizationCode) implements Result {}

  record Declined(String reason) implements Result {}

  Result authorize(String orderId, String customerId, BigDecimal amount, int attempt);
}
