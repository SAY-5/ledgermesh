package io.ledgermesh.inventory.stock;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ReservationRepository extends JpaRepository<Reservation, Long> {

  List<Reservation> findByOrderIdOrderBySkuAsc(String orderId);
}
