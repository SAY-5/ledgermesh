package io.ledgermesh.order.domain;

/** Inbound signals that move an order through the saga, including deadline expiries. */
public enum SagaEvent {
  INVENTORY_RESERVED,
  INVENTORY_REJECTED,
  PAYMENT_COMPLETED,
  PAYMENT_FAILED,
  /** No reservation outcome arrived before the reservation deadline. */
  RESERVATION_TIMEOUT,
  /** No payment outcome arrived before the payment deadline, re-drives included. */
  PAYMENT_TIMEOUT
}
