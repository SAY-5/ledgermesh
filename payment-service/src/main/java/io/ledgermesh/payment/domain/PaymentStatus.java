package io.ledgermesh.payment.domain;

public enum PaymentStatus {
  /** Recorded, authorization not yet attempted. */
  NEW,
  /** Processor was unavailable; queued for a later attempt. */
  DEFERRED,
  AUTHORIZED,
  DECLINED,
  /**
   * The order was cancelled, so nothing may be charged for it. An authorized or open payment is
   * voided and keeps no authorization: the sweeper has the processor release what it holds for the
   * order at once, and the reconciler releases any authorization of the order left over, an
   * approval that arrived after the void or whose answer never did, once it is past the grace
   * period. A cancellation that finds no payment inserts a voided marker with no amount, which the
   * processor was never asked about and which stops a later reservation or re-drive from attempting
   * one.
   */
  VOIDED;

  public boolean isOpen() {
    return this == NEW || this == DEFERRED;
  }
}
