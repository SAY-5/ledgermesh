package io.ledgermesh.common.idempotency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ledgermesh.common.idempotency.RequestDeduplicator.Outcome;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@DataJpaTest
@Import(RequestDeduplicatorTest.Config.class)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class RequestDeduplicatorTest {

  record Answer(String orderId, int sequence) {}

  static class Config {
    @Bean
    RequestDeduplicator requestDeduplicator(IdempotentRequestRepository requests) {
      return new RequestDeduplicator(
          requests, new ObjectMapper(), Clock.systemUTC(), new SimpleMeterRegistry());
    }
  }

  @Autowired private RequestDeduplicator dedup;

  @Test
  void runsTheWorkOnceAndAnswersTheRepeatFromTheStore() {
    AtomicInteger runs = new AtomicInteger();

    Outcome<Answer> first = dedup.once("key-1", Answer.class, () -> answer(runs));
    Outcome<Answer> second = dedup.once("key-1", Answer.class, () -> answer(runs));

    assertThat(first.replayed()).isFalse();
    assertThat(second.replayed()).isTrue();
    assertThat(second.body()).isEqualTo(first.body());
    assertThat(runs.get()).isEqualTo(1);
  }

  @Test
  void keepsKeysApart() {
    AtomicInteger runs = new AtomicInteger();

    dedup.once("key-2", Answer.class, () -> answer(runs));
    dedup.once("key-3", Answer.class, () -> answer(runs));

    assertThat(runs.get()).isEqualTo(2);
  }

  @Test
  void storesNothingWhenTheWorkFailsSoTheRetryRunsAgain() {
    assertThatThrownBy(
            () ->
                dedup.once(
                    "key-4",
                    Answer.class,
                    () -> {
                      throw new IllegalStateException("database hiccup");
                    }))
        .isInstanceOf(IllegalStateException.class);

    assertThat(dedup.stored("key-4", Answer.class)).isEmpty();
    assertThat(dedup.once("key-4", Answer.class, () -> answer(new AtomicInteger())).replayed())
        .isFalse();
  }

  private static Answer answer(AtomicInteger runs) {
    return new Answer("order-" + runs.incrementAndGet(), runs.get());
  }
}
