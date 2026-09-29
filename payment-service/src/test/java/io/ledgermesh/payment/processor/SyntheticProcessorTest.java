package io.ledgermesh.payment.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.ledgermesh.payment.processor.PaymentProcessor.Approved;
import io.ledgermesh.payment.processor.PaymentProcessor.Declined;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class SyntheticProcessorTest {

  private final MemoryHolds holds = new MemoryHolds();
  private final SyntheticProcessor processor =
      new SyntheticProcessor(new BigDecimal("100"), 5, 0, 0, holds);

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
    SyntheticProcessor steady = new SyntheticProcessor(new BigDecimal("100"), 0, 0, 0, holds);
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

  @Test
  void anApprovalStaysOutstandingUntilItsOrderIsReleased() {
    processor.authorize("o-held", "cust-declined", BigDecimal.ONE, 1);
    assertThat(holds.outstanding("o-held")).isZero();

    String code = firstApproval("o-held");
    firstApproval("o-held");
    assertThat(holds.outstanding("o-held")).isEqualTo(1);
    assertThat(holds.codes).containsEntry(code, "o-held");

    assertThat(processor.release("o-held")).isEqualTo(1);
    assertThat(processor.release("o-held")).isZero();
    assertThat(holds.outstanding("o-held")).isZero();

    // an approval that lands after the release is a new hold on the card, and needs its own
    firstApproval("o-held");
    assertThat(holds.outstanding("o-held")).isEqualTo(1);
    assertThat(processor.release("o-held")).isEqualTo(1);
  }

  private String firstApproval(String orderId) {
    for (int attempt = 1; attempt < 10; attempt++) {
      try {
        return ((Approved) processor.authorize(orderId, "cust", BigDecimal.TEN, attempt))
            .authorizationCode();
      } catch (ProcessorUnavailableException ignored) {
        // deterministic transient fault on this attempt, try the next one
      }
    }
    throw new AssertionError("never approved");
  }

  /** The holds a real processor would keep, in memory for a test without a database. */
  static final class MemoryHolds implements AuthorizationHolds {

    final Map<String, String> codes = new HashMap<>();
    final Map<String, Boolean> open = new HashMap<>();

    @Override
    public void grant(String orderId, String authorizationCode, BigDecimal amount) {
      codes.put(authorizationCode, orderId);
      open.put(authorizationCode, true);
    }

    @Override
    public int release(String orderId) {
      int released = 0;
      for (Map.Entry<String, String> hold : codes.entrySet()) {
        if (hold.getValue().equals(orderId) && open.get(hold.getKey())) {
          open.put(hold.getKey(), false);
          released++;
        }
      }
      return released;
    }

    @Override
    public int outstanding(String orderId) {
      return (int)
          codes.entrySet().stream()
              .filter(h -> h.getValue().equals(orderId) && open.get(h.getKey()))
              .count();
    }
  }
}
