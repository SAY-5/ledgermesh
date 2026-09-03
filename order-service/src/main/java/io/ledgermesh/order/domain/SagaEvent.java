package io.ledgermesh.order.domain;

/** Inbound signals that move an order through the saga. */
public enum SagaEvent {
  INVENTORY_RESERVED,
  INVENTORY_REJECTED,
  PAYMENT_COMPLETED,
  PAYMENT_FAILED
}
