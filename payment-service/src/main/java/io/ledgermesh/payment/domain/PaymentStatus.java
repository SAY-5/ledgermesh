package io.ledgermesh.payment.domain;

public enum PaymentStatus {
  /** Recorded, authorization not yet attempted. */
  NEW,
  /** Processor was unavailable; queued for a later attempt. */
  DEFERRED,
  AUTHORIZED,
  DECLINED;

  public boolean isOpen() {
    return this == NEW || this == DEFERRED;
  }
}
