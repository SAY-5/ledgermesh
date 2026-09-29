package io.ledgermesh.payment.processor;

import java.math.BigDecimal;

/**
 * The authorizations the synthetic processor has granted and not yet released: the stand-in for a
 * card network's own records, which a killed payment service cannot lose or roll back.
 */
public interface AuthorizationHolds {

  /** Records an authorization as outstanding; granting the same code again keeps one hold. */
  void grant(String orderId, String authorizationCode, BigDecimal amount);

  /** Releases every outstanding authorization of the order; returns how many were released. */
  int release(String orderId);

  /** How many authorizations of the order are outstanding. */
  int outstanding(String orderId);
}
