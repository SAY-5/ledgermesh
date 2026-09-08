package io.ledgermesh.order.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.time.Instant;

/**
 * One line of an order's timeline: what happened, which state it left and entered, and when. Rows
 * are appended in the same transaction as the state change they describe, so the timeline is exact
 * even across restarts. Ignored events are recorded too, with no target state.
 */
@Entity
@Table(
    name = "order_event",
    indexes = @Index(name = "ix_order_event_order", columnList = "orderId, id"))
public class OrderEvent {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(nullable = false, length = 36)
  private String orderId;

  @Column(nullable = false, length = 32)
  private String type;

  @Enumerated(EnumType.STRING)
  @Column(length = 16)
  private OrderStatus fromStatus;

  @Enumerated(EnumType.STRING)
  @Column(length = 16)
  private OrderStatus toStatus;

  @Column(length = 32)
  private String reason;

  @Column(nullable = false, length = 64)
  private String correlationId;

  @Column(nullable = false)
  private Instant occurredAt;

  protected OrderEvent() {}

  public OrderEvent(
      String orderId,
      String type,
      OrderStatus fromStatus,
      OrderStatus toStatus,
      String reason,
      String correlationId,
      Instant occurredAt) {
    this.orderId = orderId;
    this.type = type;
    this.fromStatus = fromStatus;
    this.toStatus = toStatus;
    this.reason = reason;
    this.correlationId = correlationId;
    this.occurredAt = occurredAt;
  }

  public Long getId() {
    return id;
  }

  public String getOrderId() {
    return orderId;
  }

  public String getType() {
    return type;
  }

  public OrderStatus getFromStatus() {
    return fromStatus;
  }

  public OrderStatus getToStatus() {
    return toStatus;
  }

  public String getReason() {
    return reason;
  }

  public String getCorrelationId() {
    return correlationId;
  }

  public Instant getOccurredAt() {
    return occurredAt;
  }
}
