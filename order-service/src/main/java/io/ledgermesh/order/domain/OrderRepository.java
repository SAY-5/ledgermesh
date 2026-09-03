package io.ledgermesh.order.domain;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, String> {

  long countByStatus(OrderStatus status);

  List<Order> findByStatusIn(List<OrderStatus> statuses);
}
