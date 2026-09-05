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
@Table(name = "orders", indexes = @Index(name = "ix_orders_status", columnList = "status"))
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

  @Version private long version;

  protected Order() {}

  public Order(
      String id, String customerId, List<OrderItem> items, String correlationId, Instant now) {
    this.id = id;
    this.customerId = customerId;
    this.items = new ArrayList<>(items);
    this.correlationId = correlationId;
    this.status = OrderStatus.PENDING;
    this.amount = items.stream().map(OrderItem::lineTotal).reduce(BigDecimal.ZERO, BigDecimal::add);
    this.createdAt = now;
    this.updatedAt = now;
  }

  public void transition(OrderStatus to, String reason, Instant now) {
    this.status = to;
    this.reason = reason;
    this.updatedAt = now;
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
}
