package io.ledgermesh.common.idempotency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
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

  @Test
  void aDeliveryThatMarksTheEventAfterAnotherCommittedItFailsSoItsWorkRollsBack() throws Exception {
    CountDownLatch checked = new CountDownLatch(1);
    CountDownLatch firstCommitted = new CountDownLatch(1);
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      // found no marker, then held in its work until the other delivery committed
      Future<Boolean> late =
          pool.submit(
              () ->
                  consumer.once(
                      "inventory",
                      "evt-4",
                      () -> {
                        checked.countDown();
                        waitFor(firstCommitted);
                      }));
      assertThat(checked.await(10, TimeUnit.SECONDS)).isTrue();
      assertThat(consumer.once("inventory", "evt-4", () -> {})).isTrue();
      firstCommitted.countDown();

      assertThatThrownBy(() -> late.get(10, TimeUnit.SECONDS))
          .hasCauseInstanceOf(DataIntegrityViolationException.class);
      assertThat(consumer.once("inventory", "evt-4", () -> {})).isFalse();
    } finally {
      pool.shutdownNow();
    }
  }

  private static void waitFor(CountDownLatch latch) {
    try {
      assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
