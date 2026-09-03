package io.ledgermesh.common.idempotency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
@Import(IdempotentConsumerTest.Config.class)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class IdempotentConsumerTest {

  static class Config {
    @Bean
    IdempotentConsumer idempotentConsumer(ProcessedEventRepository repo) {
      return new IdempotentConsumer(repo, Clock.systemUTC(), new SimpleMeterRegistry());
    }
  }

  @Autowired private IdempotentConsumer consumer;
  @Autowired private ProcessedEventRepository repository;

  @Test
  void runsWorkOnceForAnEventId() {
    AtomicInteger runs = new AtomicInteger();

    assertThat(consumer.once("inventory", "evt-1", runs::incrementAndGet)).isTrue();
    assertThat(consumer.once("inventory", "evt-1", runs::incrementAndGet)).isFalse();

    assertThat(runs.get()).isEqualTo(1);
    assertThat(repository.existsById("inventory:evt-1")).isTrue();
  }

  @Test
  void differentConsumersProcessTheSameEventIndependently() {
    AtomicInteger runs = new AtomicInteger();

    consumer.once("inventory", "evt-2", runs::incrementAndGet);
    consumer.once("payment", "evt-2", runs::incrementAndGet);

    assertThat(runs.get()).isEqualTo(2);
  }

  @Test
  void failingWorkLeavesNoMarkerSoRedeliveryRunsAgain() {
    AtomicInteger runs = new AtomicInteger();
    Runnable failing =
        () -> {
          runs.incrementAndGet();
          throw new IllegalStateException("database hiccup");
        };

    assertThatThrownBy(() -> consumer.once("payment", "evt-3", failing))
        .isInstanceOf(IllegalStateException.class);
    assertThat(repository.existsById("payment:evt-3")).isFalse();

    assertThat(consumer.once("payment", "evt-3", runs::incrementAndGet)).isTrue();
    assertThat(runs.get()).isEqualTo(2);
  }
}
