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

/** One payment per order. Open payments form the deferred retry queue. */
@Entity
@Table(name = "payment", indexes = @Index(name = "ix_payment_open", columnList = "status, nextAttemptAt"))
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

  @Version private long version;

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

  public void authorized(String code, Instant now) {
    this.status = PaymentStatus.AUTHORIZED;
    this.authorizationCode = code;
    this.nextAttemptAt = null;
    this.updatedAt = now;
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
}
