package io.ledgermesh.payment.authorize;

import io.ledgermesh.payment.domain.AuthorizationDecision;
import io.ledgermesh.payment.domain.AuthorizationDecisionRepository;
import java.util.function.Function;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

/** One short transaction, ordered decision → fresh payment → outbox; never provider I/O. */
@Component
public class AuthorizationDecisions {
  private final AuthorizationDecisionRepository decisions;
  private final TransactionTemplate tx;

  public AuthorizationDecisions(AuthorizationDecisionRepository decisions, TransactionTemplate tx) {
    this.decisions = decisions;
    this.tx = tx;
  }

  public <T> T execute(String code, String orderId, Function<AuthorizationDecision, T> work) {
    if (TransactionSynchronizationManager.isActualTransactionActive()) {
      throw new IllegalStateException(
          "Authorization decision requires an outer transaction boundary");
    }
    for (int attempt = 0; ; attempt++) {
      try {
        return tx.execute(
            status -> {
              AuthorizationDecision decision = decisions.lockCode(code).orElse(null);
              if (decision == null) {
                try {
                  decision = decisions.saveAndFlush(new AuthorizationDecision(code, orderId));
                } catch (DataIntegrityViolationException conflict) {
                  // In PostgreSQL a failed insert aborts this entire transaction. Never query again
                  // here. The exception escapes TransactionTemplate so rollback precedes any retry.
                  throw new CreationConflict(conflict);
                }
              }
              decision.requireOrder(orderId);
              return work.apply(decision);
            });
      } catch (CreationConflict conflict) {
        if (attempt == 4) {
          throw (DataIntegrityViolationException) conflict.getCause();
        }
      }
    }
  }

  private static final class CreationConflict extends RuntimeException {
    CreationConflict(DataIntegrityViolationException cause) {
      super(cause);
    }
  }
}
