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
   * voided and the sweeper has the processor release every authorization it holds for the order,
   * until it confirms; an approval that arrives after the void, answered late or not, a
   * cancellation sent again and a restart of the payment service each make that release due again.
   * A cancellation that finds no payment inserts a voided marker with no amount, which the
   * processor was never asked about and which stops a later reservation or re-drive from
   * attempting one.
   */
  VOIDED;

  public boolean isOpen() {
    return this == NEW || this == DEFERRED;
  }
}
