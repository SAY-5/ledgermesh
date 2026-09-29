package io.ledgermesh.payment.domain;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

public interface PaymentRepository extends JpaRepository<Payment, String> {

  List<Payment> findTop100ByStatusInAndNextAttemptAtLessThanEqualOrderByNextAttemptAtAsc(
      Collection<PaymentStatus> statuses, Instant now);

  List<Payment> findTop100ByReleaseDueAtLessThanEqualAndStatusInOrderByReleaseDueAtAsc(
      Instant now, Collection<PaymentStatus> statuses);

  long countByStatus(PaymentStatus status);

  long countByReleaseDueAtIsNotNull();

  /**
   * Records a release the processor confirmed, but only on the version of the row the release
   * started from; returns 0 when the row changed meanwhile, which leaves the release due.
   */
  @Transactional
  @Modifying(clearAutomatically = true)
  @Query(
      "update Payment p set p.releaseDueAt = null, p.releasedAt = :now, p.releaseAttempts = 0,"
          + " p.version = p.version + 1"
          + " where p.orderId = :orderId and p.version = :version and p.releaseDueAt is not null")
  int released(
      @Param("orderId") String orderId, @Param("version") Long version, @Param("now") Instant now);

  /**
   * Pushes a release the processor refused back to {@code next}, on the version of the row the
   * release started from; returns 0 when the row changed meanwhile.
   */
  @Transactional
  @Modifying(clearAutomatically = true)
  @Query(
      "update Payment p set p.releaseDueAt = :next, p.releaseAttempts = p.releaseAttempts + 1,"
          + " p.version = p.version + 1"
          + " where p.orderId = :orderId and p.version = :version and p.releaseDueAt is not null")
  int releaseRefused(
      @Param("orderId") String orderId,
      @Param("version") Long version,
      @Param("next") Instant next);
}
