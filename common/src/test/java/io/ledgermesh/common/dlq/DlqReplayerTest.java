package io.ledgermesh.common.dlq;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.ledgermesh.common.events.Topics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

class DlqReplayerTest {

  private static final String DLQ = Topics.dlq(Topics.ORDER_CREATED);
  private static final TopicPartition P0 = new TopicPartition(DLQ, 0);

  // the replayer closes its consumer; the test still needs to read what was committed afterwards
  private final MockConsumer<String, String> consumer =
      new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
        @Override
        public synchronized void close(Duration timeout) {}
      };

  @SuppressWarnings("unchecked")
  private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);

  private final MutableClock clock = new MutableClock();
  private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

  @Test
  void commitsOnlyTheRecordsItHandledWhenTheBudgetEndsMidBatch() {
    DlqReplayer replayer = replayer(3);
    consumer.schedulePollTask(this::assign);
    consumer.schedulePollTask(() -> consumer.addRecord(deadLetter(0, 0)));
    consumer.schedulePollTask(
        () -> {
          consumer.addRecord(deadLetter(1, 0));
          consumer.addRecord(deadLetter(2, 0));
          consumer.addRecord(deadLetter(3, 0));
        });

    DlqReplayer.Replayed outcome = replayer.replay(Topics.ORDER_CREATED, 3);

    assertThat(outcome).isEqualTo(new DlqReplayer.Replayed(3, 0));
    assertThat(sent())
        .extracting(ProducerRecord::key)
        .containsExactly("order-0", "order-1", "order-2");
    assertThat(sent()).extracting(ProducerRecord::topic).containsOnly(Topics.ORDER_CREATED);
    // offset 3 was polled but not handled, so the next replay has to see it again
    assertThat(consumer.committed(Set.of(P0)).get(P0).offset()).isEqualTo(3);
  }

  @Test
  void parksARecordThatUsedUpItsReplaysOnTheParkedTopic() {
    DlqReplayer replayer = replayer(1);
    consumer.schedulePollTask(this::assign);
    consumer.schedulePollTask(() -> consumer.addRecord(deadLetter(0, 1)));
    consumer.schedulePollTask(() -> clock.advance(Duration.ofSeconds(3)));

    DlqReplayer.Replayed outcome = replayer.replay(Topics.ORDER_CREATED, 10);

    assertThat(outcome).isEqualTo(new DlqReplayer.Replayed(0, 1));
    ProducerRecord<String, String> parked = sent().get(0);
    assertThat(parked.topic()).isEqualTo(Topics.parked(Topics.ORDER_CREATED));
    assertThat(parked.key()).isEqualTo("order-0");
    assertThat(
            new String(
                parked.headers().lastHeader(PoisonMessagePolicy.REPLAY_COUNT_HEADER).value(),
                UTF_8))
        .isEqualTo("1");
    assertThat(parked.headers().lastHeader(DlqReplayer.PARKED_AT_HEADER)).isNotNull();
    assertThat(consumer.committed(Set.of(P0)).get(P0).offset()).isEqualTo(1);
    assertThat(meters.counter("ledgermesh.dlq.parked", "topic", Topics.ORDER_CREATED).count())
        .isEqualTo(1);
  }

  @Test
  void countsIdleTimeOnlyAfterTheGroupHandedOutPartitions() {
    DlqReplayer replayer = replayer(3);
    // the join alone takes longer than the idle window; the record shows up right after it
    consumer.schedulePollTask(
        () -> {
          clock.advance(Duration.ofSeconds(4));
          assign();
        });
    consumer.schedulePollTask(() -> consumer.addRecord(deadLetter(0, 0)));

    assertThat(replayer.replay(Topics.ORDER_CREATED, 1)).isEqualTo(new DlqReplayer.Replayed(1, 0));
  }

  private DlqReplayer replayer(int maxReplays) {
    when(kafka.send(any(ProducerRecord.class)))
        .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));
    ConsumerFactory<String, String> consumers =
        new ConsumerFactory<>() {
          @Override
          public Consumer<String, String> createConsumer(
              String groupId, String clientIdPrefix, String clientIdSuffix, Properties properties) {
            return consumer;
          }

          @Override
          public boolean isAutoCommit() {
            return false;
          }
        };
    return new DlqReplayer(
        consumers, kafka, "inventory-service", clock, meters, new PoisonMessagePolicy(maxReplays));
  }

  private void assign() {
    consumer.rebalance(List.of(P0));
    consumer.updateBeginningOffsets(Map.of(P0, 0L));
  }

  private List<ProducerRecord<String, String>> sent() {
    ArgumentCaptor<ProducerRecord<String, String>> captor =
        ArgumentCaptor.forClass(ProducerRecord.class);
    verify(kafka, atLeast(0)).send(captor.capture());
    return captor.getAllValues();
  }

  private static ConsumerRecord<String, String> deadLetter(long offset, int replays) {
    ConsumerRecord<String, String> record =
        new ConsumerRecord<>(DLQ, 0, offset, "order-" + offset, "{}");
    if (replays > 0) {
      record
          .headers()
          .add(PoisonMessagePolicy.REPLAY_COUNT_HEADER, Integer.toString(replays).getBytes(UTF_8));
    }
    return record;
  }

  static final class MutableClock extends Clock {
    private Instant now = Instant.parse("2026-01-01T00:00:00Z");

    void advance(Duration by) {
      now = now.plus(by);
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return now;
    }
  }
}
