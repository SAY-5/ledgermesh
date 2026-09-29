package io.ledgermesh.payment.processor;

import java.math.BigDecimal;
import java.util.List;

/**
 * The authorizations the synthetic processor has granted and not yet released: the stand-in for a
 * card network's own records, which a killed payment service cannot lose or roll back.
 */
public interface AuthorizationHolds {

  /** Records a new outstanding authorization under its own code. */
  void grant(String orderId, String authorizationCode, BigDecimal amount);

  /**
   * Releases every outstanding authorization of the order but the one whose code is {@code keep}
   * (none when null); returns how many were released.
   */
  int release(String orderId, String keep);

  /** Releases one authorization by its code; returns whether it was outstanding. */
  boolean releaseCode(String orderId, String authorizationCode);

  /** How many authorizations of the order are outstanding. */
  int outstanding(String orderId);

  /**
   * Outstanding authorizations in grant order (grant time, then code), at most {@code limit},
   * starting after {@code after} (from the first when null).
   */
  List<PaymentProcessor.Authorization> outstanding(PaymentProcessor.Authorization after, int limit);
}
