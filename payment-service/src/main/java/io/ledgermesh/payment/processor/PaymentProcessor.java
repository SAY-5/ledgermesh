package io.ledgermesh.payment.processor;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * The external card processor. Throws {@link ProcessorUnavailableException} on transient faults.
 *
 * <p>Besides authorizing, the payment service needs from a processor what real processors offer for
 * reconciliation: a listing of the merchant's outstanding authorizations, each with the merchant's
 * reference (the order id), its code and when it was granted; the release of one authorization by
 * its code; and the release of every authorization under a reference but one. With those, the
 * payment service's reconciler releases every authorization its payment does not keep once it is
 * older than the longest call plus a commit, whatever became of the answer.
 */
public interface PaymentProcessor {

  sealed interface Result permits Approved, Declined {}

  record Approved(String authorizationCode) implements Result {}

  record Declined(String reason) implements Result {}

  /** An outstanding authorization as the processor lists it. */
  record Authorization(
      String orderId, String authorizationCode, BigDecimal amount, Instant grantedAt) {}

  /**
   * Asks for an authorization. Every approval is a new authorization with a code of its own, so two
   * approvals for one order, a timed out attempt and its retry say, are two holds on the card.
   */
  Result authorize(String orderId, String customerId, BigDecimal amount, int attempt);

  /**
   * Releases every authorization outstanding under the order's reference except the one whose code
   * is {@code keep} (none when {@code keep} is null), and returns how many it released. Idempotent:
   * once nothing but {@code keep} is outstanding, a release releases nothing.
   */
  int release(String orderId, String keep);

  /** Releases one authorization by its code; returns whether it was outstanding. Idempotent. */
  boolean releaseAuthorization(String orderId, String authorizationCode);

  /**
   * The merchant's outstanding authorizations in grant order, at most {@code limit} of them,
   * starting after {@code after} (from the first when null), so a caller can page through all of
   * them.
   */
  List<Authorization> outstanding(Authorization after, int limit);

  /**
   * The longest a call can run before the processor answers or gives up, which is the bound the
   * client enforces on it, not how long the payment service waits (the time limiter cuts the wait,
   * not the call). The reconciler adds it to the time it leaves an authorization alone.
   */
  Duration longestCall();
}
