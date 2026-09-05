package io.ledgermesh.order.domain;

import java.util.Optional;

/**
 * Pure transition table for the order saga.
 *
 * <pre>
 * PENDING  + INVENTORY_RESERVED -> RESERVED
 * PENDING  + INVENTORY_REJECTED -> CANCELLED (OUT_OF_STOCK)
 * RESERVED + PAYMENT_COMPLETED  -> CONFIRMED
 * RESERVED + PAYMENT_FAILED     -> CANCELLED (PAYMENT_DECLINED) and release the reservation
 * </pre>
 *
 * Payment outcomes arriving before the reservation notice (the two topics are independent) are
 * accepted as well, because a payment can only exist for a reserved order. Anything else, including
 * any event on a terminal order, is ignored: redelivered or late events never move an order
 * backwards.
 */
public final class OrderStateMachine {

  public static final String OUT_OF_STOCK = "OUT_OF_STOCK";
  public static final String PAYMENT_DECLINED = "PAYMENT_DECLINED";

  /** Outcome of applying an event. {@code releaseInventory} requests compensation. */
  public record Transition(OrderStatus to, String reason, boolean releaseInventory) {}

  private OrderStateMachine() {}

  public static Optional<Transition> apply(OrderStatus current, SagaEvent event) {
    if (current.isTerminal()) {
      return Optional.empty();
    }
    return switch (event) {
      case INVENTORY_RESERVED ->
          current == OrderStatus.PENDING
              ? Optional.of(new Transition(OrderStatus.RESERVED, null, false))
              : Optional.empty();
      case INVENTORY_REJECTED ->
          current == OrderStatus.PENDING
              ? Optional.of(new Transition(OrderStatus.CANCELLED, OUT_OF_STOCK, false))
              : Optional.empty();
      case PAYMENT_COMPLETED -> Optional.of(new Transition(OrderStatus.CONFIRMED, null, false));
      case PAYMENT_FAILED ->
          Optional.of(new Transition(OrderStatus.CANCELLED, PAYMENT_DECLINED, true));
    };
  }
}
