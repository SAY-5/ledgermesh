package io.ledgermesh.payment.authorize;

/**
 * What the processor answered to a call nobody was waiting for any more: the time limiter had cut
 * it off, but the call ran on and the processor acted on it. Published so the answer still reaches
 * the payment, where an approval that the payment does not keep is released.
 */
public record LateOutcome(String orderId, AuthorizationOutcome outcome) {}
