package io.ledgermesh.order.domain;

import java.time.Instant;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, String> {

  long countByStatus(OrderStatus status);

  List<Order> findByStatusIn(List<OrderStatus> statuses);

  List<Order> findTop100ByStatusInAndDeadlineAtLessThanEqualOrderByDeadlineAtAsc(
      List<OrderStatus> statuses, Instant now);
}
