package io.ledgermesh.payment.domain;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PaymentRepository extends JpaRepository<Payment, String> {

  List<Payment> findTop100ByStatusInAndNextAttemptAtLessThanEqualOrderByNextAttemptAtAsc(
      Collection<PaymentStatus> statuses, Instant now);

  long countByStatus(PaymentStatus status);
}
