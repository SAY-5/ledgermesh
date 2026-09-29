package io.ledgermesh.payment.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.math.BigDecimal;
import java.time.Instant;
import org.hibernate.annotations.ColumnDefault;

/** One payment per order. Open payments form the deferred retry queue. */
@Entity
@Table(
    name = "payment",
    indexes = @Index(name = "ix_payment_open", columnList = "status, nextAttemptAt"))
public class Payment {

  @Id
  @Column(length = 36)
  private String orderId;

  @Column(nullable = false, length = 64)
  private String customerId;

  @Column(nullable = false, precision = 12, scale = 2)
  private BigDecimal amount;

  @Enumerated(EnumType.STRING)
  @Column(nullable = false, length = 16)
  private PaymentStatus status;

  @Column(length = 64)
  private String authorizationCode;

  @Column(length = 64)
  private String reason;

  @Column(nullable = false)
  private int attempts;

  private Instant nextAttemptAt;

  @Column(nullable = false, length = 64)
  private String correlationId;

  @Column(nullable = false)
  private Instant createdAt;

  @Column(nullable = false)
  private Instant updatedAt;

  /**
   * Set while the processor may hold an authorization for the order that this payment does not keep
   * (a payment keeps its own authorization while it is authorized, and nothing otherwise), and has
   * not confirmed it released the rest: from an authorization, a void or a cancellation sent again,
   * and from every approval the payment does not keep. Cleared only by a release that started from
   * the version of the row it clears, so an approval that lands during a release leaves the next
   * one due; pushed back after each release the processor refused.
   */
  private Instant releaseDueAt;

  /**
   * When the processor last confirmed it holds nothing for this order the payment does not keep.
   */
  private Instant releasedAt;

  /** Releases the processor refused in a row, which sets how long the next one waits. */
  @ColumnDefault("0")
  @Column(nullable = false)
  private int releaseAttempts;

  /**
   * Null until the payment is inserted, which is how the repository tells a new payment from a
   * stored one: the id is the order id, assigned rather than generated. A primitive version would
   * make it merge a new payment, and a merge reads the row first, so a creator that saves after
   * another has committed the same order, before anything has attempted that payment, would
   * overwrite it instead of failing on the primary key.
   */
  @Version
  @Column(nullable = false)
  private Long version;

  protected Payment() {}

  public Payment(
      String orderId,
      String customerId,
      BigDecimal amount,
      String correlationId,
      Instant now,
      Instant firstAttemptDeadline) {
    this.orderId = orderId;
    this.customerId = customerId;
    this.amount = amount;
    this.correlationId = correlationId;
    this.status = PaymentStatus.NEW;
    this.createdAt = now;
    this.updatedAt = now;
    this.nextAttemptAt = firstAttemptDeadline;
  }

  /**
   * The trace a cancellation leaves when no payment was on file for the order: voided, for nothing,
   * never attempted, so there is nothing at the processor to release. Whichever creator comes later
   * finds it and leaves the order uncharged.
   */
  public static Payment voidedMarker(
      String orderId, String correlationId, String reason, Instant now) {
    Payment marker = new Payment(orderId, "", BigDecimal.ZERO, correlationId, now, null);
    marker.voided(reason, now);
    marker.releaseDueAt = null;
    return marker;
  }

  /**
   * Keeps this authorization, and makes a release of any other the processor holds for the order
   * due, such as one granted to an attempt whose answer was lost with a killed payment service.
   */
  public void authorized(String code, Instant now) {
    this.status = PaymentStatus.AUTHORIZED;
    this.authorizationCode = code;
    this.nextAttemptAt = null;
    this.updatedAt = now;
    this.releaseDueAt = now;
  }

  public void declined(String reason, Instant now) {
    this.status = PaymentStatus.DECLINED;
    this.reason = reason;
    this.nextAttemptAt = null;
    this.updatedAt = now;
  }

  public void deferred(String reason, Instant retryAt, Instant now) {
    this.status = PaymentStatus.DEFERRED;
    this.reason = reason;
    this.nextAttemptAt = retryAt;
    this.updatedAt = now;
  }

  /**
   * Gives the payment up for a cancelled order and makes a release at the processor due: an
   * authorized payment holds money on the card, and an open one may have been approved by an
   * attempt whose answer never made it here. An authorization code stays as the record.
   */
  public void voided(String reason, Instant now) {
    this.status = PaymentStatus.VOIDED;
    this.reason = reason;
    this.nextAttemptAt = null;
    this.updatedAt = now;
    this.releaseDueAt = now;
  }

  /**
   * The processor may hold an authorization this payment does not keep: an approval that landed
   * after the void or besides the one kept, or a cancellation sent again that asks to check.
   */
  public void releaseOwed(Instant now) {
    this.updatedAt = now;
    this.releaseDueAt = now;
  }

  /** The authorization the processor is told to keep when it releases, if any. */
  public String keptAuthorization() {
    return status == PaymentStatus.AUTHORIZED ? authorizationCode : null;
  }

  public void attempted() {
    this.attempts++;
  }

  public String getOrderId() {
    return orderId;
  }

  public String getCustomerId() {
    return customerId;
  }

  public BigDecimal getAmount() {
    return amount;
  }

  public PaymentStatus getStatus() {
    return status;
  }

  public String getAuthorizationCode() {
    return authorizationCode;
  }

  public String getReason() {
    return reason;
  }

  public int getAttempts() {
    return attempts;
  }

  public Instant getNextAttemptAt() {
    return nextAttemptAt;
  }

  public String getCorrelationId() {
    return correlationId;
  }

  public Instant getCreatedAt() {
    return createdAt;
  }

  public Instant getReleaseDueAt() {
    return releaseDueAt;
  }

  public Instant getReleasedAt() {
    return releasedAt;
  }

  public int getReleaseAttempts() {
    return releaseAttempts;
  }

  public Long getVersion() {
    return version;
  }
}
