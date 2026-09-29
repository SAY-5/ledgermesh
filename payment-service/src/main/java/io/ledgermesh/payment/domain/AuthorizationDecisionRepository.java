package io.ledgermesh.payment.domain;

import jakarta.persistence.LockModeType;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface AuthorizationDecisionRepository
    extends JpaRepository<AuthorizationDecision, String> {
  @Lock(LockModeType.PESSIMISTIC_WRITE)
  @Query("select d from AuthorizationDecision d where d.authorizationCode = :code")
  Optional<AuthorizationDecision> lockCode(@Param("code") String code);
}
