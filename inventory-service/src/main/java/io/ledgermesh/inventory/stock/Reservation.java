package io.ledgermesh.inventory.stock;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import java.time.Instant;

/**
 * One line of one order's hold on stock. Written in the same transaction as the decrement, so the
 * ledger and the stock row can never disagree. A release flips the state and credits the quantity
 * recorded here, never the quantity an event claims; a release that finds no rows leaves a released
 * marker with quantity zero, which is what stops a late reservation for the same order.
 */
@Entity
@Table(
    name = "reservation",
    indexes = @Index(name = "ix_reservation_order", columnList = "order_id"),
    uniqueConstraints =
        @UniqueConstraint(
            name = "uk_reservation_order_sku",
            columnNames = {"order_id", "sku"}))
public class Reservation {

  public enum State {
    RESERVED,
    RELEASED
  }

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(name = "order_id", nullable = false, length = 64)
  private String orderId;

  @Column(name = "sku", nullable = false, length = 64)
  private String sku;

  @Column(nullable = false)
  private int quantity;

  @Enumerated(EnumType.STRING)
  @Column(nullable = false, length = 16)
  private State state;

  @Column(nullable = false)
  private Instant createdAt;

  private Instant releasedAt;

  protected Reservation() {}

  private Reservation(String orderId, String sku, int quantity, State state, Instant now) {
    this.orderId = orderId;
    this.sku = sku;
    this.quantity = quantity;
    this.state = state;
    this.createdAt = now;
    this.releasedAt = state == State.RELEASED ? now : null;
  }

  static Reservation reserved(String orderId, String sku, int quantity, Instant now) {
    return new Reservation(orderId, sku, quantity, State.RESERVED, now);
  }

  /** The trace a release leaves when nothing had been reserved for the order. */
  static Reservation releasedMarker(String orderId, String sku, Instant now) {
    return new Reservation(orderId, sku, 0, State.RELEASED, now);
  }

  void release(Instant now) {
    this.state = State.RELEASED;
    this.releasedAt = now;
  }

  public String getOrderId() {
    return orderId;
  }

  public String getSku() {
    return sku;
  }

  public int getQuantity() {
    return quantity;
  }

  public State getState() {
    return state;
  }

  public Instant getCreatedAt() {
    return createdAt;
  }

  public Instant getReleasedAt() {
    return releasedAt;
  }
}
