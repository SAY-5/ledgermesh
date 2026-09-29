package io.ledgermesh.order.domain;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, String> {

  long countByStatus(OrderStatus status);

  List<Order> findByStatusIn(List<OrderStatus> statuses);

  long countByStatusInAndDeadlineAtLessThanEqual(List<OrderStatus> statuses, Instant now);

  List<Order> findTop100ByStatusInAndDeadlineAtLessThanEqualOrderByDeadlineAtAsc(
      List<OrderStatus> statuses, Instant now);

  /** Cancellations the payment service has not answered in time. */
  List<Order>
      findTop100ByCompensatedAtIsNullAndCompensationDueAtLessThanEqualOrderByCompensationDueAtAsc(
          Instant now);

  long countByCompensatedAtIsNullAndCompensationDueAtLessThanEqual(Instant now);

  /**
   * Orders cancelled for one of these reasons whose cancellation was never answered and is not
   * waiting for an answer either: cancelled before the order service waited for answers.
   */
  List<Order>
      findByStatusAndReasonInAndCompensatedAtIsNullAndCompensationDueAtIsNullOrderByUpdatedAtAsc(
          OrderStatus status, Collection<String> reasons, Pageable page);
}
