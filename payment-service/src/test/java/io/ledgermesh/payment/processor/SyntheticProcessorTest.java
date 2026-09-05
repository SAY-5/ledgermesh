package io.ledgermesh.payment.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.ledgermesh.payment.processor.PaymentProcessor.Approved;
import io.ledgermesh.payment.processor.PaymentProcessor.Declined;
import java.math.BigDecimal;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class SyntheticProcessorTest {

  private final SyntheticProcessor processor = new SyntheticProcessor(new BigDecimal("100"), 5, 0, 0);

  @Test
  void declinesFlaggedCustomersAndAmountsOverTheLimit() {
    assertThat(processor.authorize("o1", "cust-declined", BigDecimal.ONE, 1))
        .isEqualTo(new Declined("CARD_DECLINED"));
    assertThat(processor.authorize("o1", "cust", new BigDecimal("100.01"), 1))
        .isEqualTo(new Declined("OVER_LIMIT"));
  }

  @Test
  void approvalCodeIsStableForAnOrder() {
    String first = firstApproval("order-stable");
    assertThat(first).startsWith("AUTH-");
    assertThat(firstApproval("order-stable")).isEqualTo(first);
  }

  @Test
  void transientFaultsHitTheConfiguredShareOfFirstAttempts() {
    long faults =
        IntStream.range(0, 2000)
            .filter(
                i -> {
                  try {
                    processor.authorize("order-" + i, "cust", BigDecimal.TEN, 1);
                    return false;
                  } catch (ProcessorUnavailableException e) {
                    return true;
                  }
                })
            .count();
    assertThat(faults).isBetween(60L, 140L);
  }

  @Test
  void noFaultsWhenTheShareIsZero() {
    SyntheticProcessor steady = new SyntheticProcessor(new BigDecimal("100"), 0, 0, 0);
    IntStream.range(0, 500).forEach(i -> steady.authorize("order-" + i, "cust", BigDecimal.TEN, 1));
  }

  @Test
  void faultAlwaysThrowsForTheSameOrderAndAttempt() {
    String faulty =
        IntStream.range(0, 1000)
            .mapToObj(i -> "order-" + i)
            .filter(id -> SyntheticProcessor.bucket(id + ":1") < 5)
            .findFirst()
            .orElseThrow();
    assertThatThrownBy(() -> processor.authorize(faulty, "cust", BigDecimal.TEN, 1))
        .isInstanceOf(ProcessorUnavailableException.class);
    assertThatThrownBy(() -> processor.authorize(faulty, "cust", BigDecimal.TEN, 1))
        .isInstanceOf(ProcessorUnavailableException.class);
  }

  private String firstApproval(String orderId) {
    for (int attempt = 1; attempt < 10; attempt++) {
      try {
        return ((Approved) processor.authorize(orderId, "cust", BigDecimal.TEN, attempt)).authorizationCode();
      } catch (ProcessorUnavailableException ignored) {
        // deterministic transient fault on this attempt, try the next one
      }
    }
    throw new AssertionError("never approved");
  }
}
