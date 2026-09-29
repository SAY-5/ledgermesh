package io.ledgermesh.payment.domain;

public enum PaymentStatus {
  /** Recorded, authorization not yet attempted. */
  NEW,
  /** Processor was unavailable; queued for a later attempt. */
  DEFERRED,
  AUTHORIZED,
  DECLINED,
  /**
   * The order was cancelled: an authorization is given back, an open payment is never attempted,
   * and a payment that did not exist yet is blocked by a marker that holds nothing.
   */
  VOIDED;

  public boolean isOpen() {
    return this == NEW || this == DEFERRED;
  }
}
