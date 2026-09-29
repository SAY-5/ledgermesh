package io.ledgermesh.order.domain;

import jakarta.persistence.CollectionTable;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(
    name = "orders",
    indexes = {
      @Index(name = "ix_orders_status", columnList = "status"),
      @Index(name = "ix_orders_deadline", columnList = "status, deadlineAt")
    })
public class Order {

  @Id
  @Column(length = 36)
  private String id;

  @Column(nullable = false, length = 64)
  private String customerId;

  @Enumerated(EnumType.STRING)
  @Column(nullable = false, length = 16)
  private OrderStatus status;

  @Column(length = 32)
  private String reason;

  @Column(nullable = false, precision = 12, scale = 2)
  private BigDecimal amount;

  @Column(nullable = false, length = 64)
  private String correlationId;

  @ElementCollection(fetch = FetchType.EAGER)
  @CollectionTable(name = "order_item", joinColumns = @JoinColumn(name = "order_id"))
  private List<OrderItem> items = new ArrayList<>();

  @Column(nullable = false)
  private Instant createdAt;

  @Column(nullable = false)
  private Instant updatedAt;

  /** When the current saga step must have answered; null once the order is terminal. */
  private Instant deadlineAt;

  @Column(nullable = false)
  private int redrives;

  /**
   * When {@code order.cancelled} is sent again unless the payment service has answered it by then;
   * null when no cancellation is waiting for an answer.
   */
  private Instant compensationDueAt;

  /** When the payment service answered the cancellation with {@code payment.voided}. */
  private Instant compensatedAt;

  /**
   * Null until the order is inserted. The id is assigned, so the version is how the repository
   * tells a new order from a stored one; a primitive one would make it merge a new order, reading
   * the row before inserting it.
   */
  @Version
  @Column(nullable = false)
  private Long version;

  protected Order() {}

  public Order(
      String id,
      String customerId,
      List<OrderItem> items,
      String correlationId,
      Instant now,
      Instant deadlineAt) {
    this.id = id;
    this.customerId = customerId;
    this.items = new ArrayList<>(items);
    this.correlationId = correlationId;
    this.status = OrderStatus.PENDING;
    this.amount = items.stream().map(OrderItem::lineTotal).reduce(BigDecimal.ZERO, BigDecimal::add);
    this.createdAt = now;
    this.updatedAt = now;
    this.deadlineAt = deadlineAt;
  }

  public void transition(OrderStatus to, String reason, Instant now, Instant deadlineAt) {
    this.status = to;
    this.reason = reason;
    this.updatedAt = now;
    this.deadlineAt = deadlineAt;
  }

  /** Waits for the payment service to answer the cancellation, sent again at {@code due}. */
  public void awaitCompensation(Instant due) {
    this.compensationDueAt = due;
  }

  /** The payment service answered the cancellation: nothing is charged for the order. */
  public void compensated(Instant now) {
    this.compensatedAt = now;
    this.compensationDueAt = null;
  }

  /** Extends the current step's deadline after a re-drive was requested. */
  public void redriven(Instant now, Instant deadlineAt) {
    this.redrives++;
    this.updatedAt = now;
    this.deadlineAt = deadlineAt;
  }

  public String getId() {
    return id;
  }

  public String getCustomerId() {
    return customerId;
  }

  public OrderStatus getStatus() {
    return status;
  }

  public String getReason() {
    return reason;
  }

  public BigDecimal getAmount() {
    return amount;
  }

  public String getCorrelationId() {
    return correlationId;
  }

  public List<OrderItem> getItems() {
    return items;
  }

  public Instant getCreatedAt() {
    return createdAt;
  }

  public Instant getUpdatedAt() {
    return updatedAt;
  }

  public Instant getDeadlineAt() {
    return deadlineAt;
  }

  public int getRedrives() {
    return redrives;
  }

  public Instant getCompensationDueAt() {
    return compensationDueAt;
  }

  public Instant getCompensatedAt() {
    return compensatedAt;
  }
}
