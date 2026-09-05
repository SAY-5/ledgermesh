package io.ledgermesh.payment.authorize;

/** What the authorizer concluded after retries, the breaker and the time limiter had their say. */
public sealed interface AuthorizationOutcome
    permits AuthorizationOutcome.Authorized,
        AuthorizationOutcome.Declined,
        AuthorizationOutcome.Deferred {

  record Authorized(String code) implements AuthorizationOutcome {}

  record Declined(String reason) implements AuthorizationOutcome {}

  /** The processor could not be reached reliably; try again later, never drop the payment. */
  record Deferred(String reason) implements AuthorizationOutcome {}
}
