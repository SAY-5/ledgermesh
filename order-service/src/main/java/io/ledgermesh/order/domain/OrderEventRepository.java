package io.ledgermesh.order.domain;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderEventRepository extends JpaRepository<OrderEvent, Long> {

  List<OrderEvent> findByOrderIdOrderByIdAsc(String orderId);
}
