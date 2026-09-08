package io.ledgermesh.order.saga;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Per step deadlines for the saga. {@code reservation} bounds the wait for an inventory outcome,
 * {@code payment} bounds each wait for a payment outcome, and {@code maxRedrives} says how many
 * times a silent payment is asked for again before the order is cancelled.
 */
@ConfigurationProperties(prefix = "ledgermesh.saga")
public record SagaTimeouts(Duration reservation, Duration payment, int maxRedrives) {

  public SagaTimeouts {
    reservation = reservation == null ? Duration.ofSeconds(60) : reservation;
    payment = payment == null ? Duration.ofSeconds(120) : payment;
  }
}
