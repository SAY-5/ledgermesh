package io.ledgermesh.payment.authorize;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;

/** Blocking facade over the decorated processor call. */
@Component
public class PaymentAuthorizer {

  public static final String RESILIENCE_NAME = ResilientProcessorCall.RESILIENCE_NAME;

  private final ResilientProcessorCall call;

  public PaymentAuthorizer(ResilientProcessorCall call) {
    this.call = call;
  }

  public AuthorizationOutcome authorize(String orderId, String customerId, BigDecimal amount) {
    return call.authorize(orderId, customerId, amount, new AtomicInteger()).join();
  }

  /**
   * Releases what the processor holds for the order but {@code keep}; returns how many it released.
   * Throws when the processor could not be reached or did not answer in time.
   */
  public int release(String orderId, String keep) {
    return call.release(orderId, keep).join();
  }

  /** Background retry from the deferred queue: same breaker, longer time budget. */
  public AuthorizationOutcome authorizeDeferred(
      String orderId, String customerId, BigDecimal amount, int priorAttempts) {
    return call.authorizeDeferred(orderId, customerId, amount, new AtomicInteger(priorAttempts))
        .join();
  }
}
