package io.ledgermesh.order.saga;

import static org.assertj.core.api.Assertions.assertThat;

import io.ledgermesh.order.domain.OrderStateMachine;
import io.ledgermesh.order.domain.OrderStateMachine.Transition;
import io.ledgermesh.order.domain.OrderStatus;
import io.ledgermesh.order.domain.SagaEvent;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class OrderStateMachineTest {

  @Test
  void happyPathReservesThenConfirms() {
    Transition reserved = OrderStateMachine.apply(OrderStatus.PENDING, SagaEvent.INVENTORY_RESERVED).orElseThrow();
    assertThat(reserved.to()).isEqualTo(OrderStatus.RESERVED);
    assertThat(reserved.releaseInventory()).isFalse();

    Transition confirmed = OrderStateMachine.apply(reserved.to(), SagaEvent.PAYMENT_COMPLETED).orElseThrow();
    assertThat(confirmed.to()).isEqualTo(OrderStatus.CONFIRMED);
    assertThat(confirmed.reason()).isNull();
  }

  @Test
  void outOfStockCancelsWithoutCompensation() {
    Transition t = OrderStateMachine.apply(OrderStatus.PENDING, SagaEvent.INVENTORY_REJECTED).orElseThrow();
    assertThat(t.to()).isEqualTo(OrderStatus.CANCELLED);
    assertThat(t.reason()).isEqualTo(OrderStateMachine.OUT_OF_STOCK);
    assertThat(t.releaseInventory()).isFalse();
  }

  @Test
  void declinedPaymentCancelsAndReleasesReservation() {
    Transition t = OrderStateMachine.apply(OrderStatus.RESERVED, SagaEvent.PAYMENT_FAILED).orElseThrow();
    assertThat(t.to()).isEqualTo(OrderStatus.CANCELLED);
    assertThat(t.reason()).isEqualTo(OrderStateMachine.PAYMENT_DECLINED);
    assertThat(t.releaseInventory()).isTrue();
  }

  @Test
  void paymentOutcomeArrivingBeforeReservationNoticeIsAccepted() {
    assertThat(OrderStateMachine.apply(OrderStatus.PENDING, SagaEvent.PAYMENT_COMPLETED))
        .map(Transition::to)
        .contains(OrderStatus.CONFIRMED);
  }

  @Test
  void lateReservationEventsAreIgnoredOnceReserved() {
    assertThat(OrderStateMachine.apply(OrderStatus.RESERVED, SagaEvent.INVENTORY_RESERVED)).isEmpty();
    assertThat(OrderStateMachine.apply(OrderStatus.RESERVED, SagaEvent.INVENTORY_REJECTED)).isEmpty();
  }

  @ParameterizedTest
  @EnumSource(SagaEvent.class)
  void terminalOrdersNeverMove(SagaEvent event) {
    assertThat(OrderStateMachine.apply(OrderStatus.CONFIRMED, event)).isEqualTo(Optional.empty());
    assertThat(OrderStateMachine.apply(OrderStatus.CANCELLED, event)).isEqualTo(Optional.empty());
  }
}
