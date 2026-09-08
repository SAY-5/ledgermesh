package io.ledgermesh.order.domain;

import java.util.Optional;

/**
 * Pure transition table for the order saga.
 *
 * <pre>
 * PENDING  + INVENTORY_RESERVED  -> RESERVED
 * PENDING  + INVENTORY_REJECTED  -> CANCELLED (OUT_OF_STOCK)
 * PENDING  + RESERVATION_TIMEOUT -> CANCELLED (RESERVATION_TIMEOUT) and release the reservation
 * RESERVED + PAYMENT_COMPLETED   -> CONFIRMED
 * RESERVED + PAYMENT_FAILED      -> CANCELLED (PAYMENT_DECLINED) and release the reservation
 * RESERVED + PAYMENT_TIMEOUT     -> CANCELLED (PAYMENT_TIMEOUT) and release the reservation
 * </pre>
 *
 * Payment outcomes arriving before the reservation notice (the two topics are independent) are
 * accepted as well, because a payment can only exist for a reserved order. Anything else, including
 * any event on a terminal order, is ignored: redelivered or late events never move an order
 * backwards. A reservation timeout releases stock because inventory may have reserved without the
 * notice reaching this service; the release is harmless when nothing was reserved.
 */
public final class OrderStateMachine {

  public static final String OUT_OF_STOCK = "OUT_OF_STOCK";
  public static final String PAYMENT_DECLINED = "PAYMENT_DECLINED";
  public static final String RESERVATION_TIMEOUT = "RESERVATION_TIMEOUT";
  public static final String PAYMENT_TIMEOUT = "PAYMENT_TIMEOUT";

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
      case RESERVATION_TIMEOUT ->
          current == OrderStatus.PENDING
              ? Optional.of(new Transition(OrderStatus.CANCELLED, RESERVATION_TIMEOUT, true))
              : Optional.empty();
      case PAYMENT_COMPLETED -> Optional.of(new Transition(OrderStatus.CONFIRMED, null, false));
      case PAYMENT_FAILED ->
          Optional.of(new Transition(OrderStatus.CANCELLED, PAYMENT_DECLINED, true));
      case PAYMENT_TIMEOUT ->
          current == OrderStatus.RESERVED
              ? Optional.of(new Transition(OrderStatus.CANCELLED, PAYMENT_TIMEOUT, true))
              : Optional.empty();
    };
  }
}
