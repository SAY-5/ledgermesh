package io.ledgermesh.common.outbox;

import io.ledgermesh.common.correlation.CorrelationId;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Polls unpublished outbox rows and pushes them to Kafka one at a time, in insertion order. A row
 * is stamped as attempted before the send and marked published only after the broker acknowledged
 * it. A row that comes back attempted but unpublished is exactly the crash window between the two:
 * it may or may not have reached the broker, so it is sent again and counted as a re-send.
 * Consumers de-duplicate on event id, which is what keeps the effect single.
 */
public class OutboxRelay {

  private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

  private final OutboxEventRepository repository;
  private final KafkaTemplate<String, String> kafka;
  private final Clock clock;
  private final long sendTimeoutMs;
  private final Counter published;
  private final Counter failures;
  private final Counter resends;

  public OutboxRelay(
      OutboxEventRepository repository,
      KafkaTemplate<String, String> kafka,
      Clock clock,
      MeterRegistry registry,
      long sendTimeoutMs) {
    this.repository = repository;
    this.kafka = kafka;
    this.clock = clock;
    this.sendTimeoutMs = sendTimeoutMs;
    this.published = registry.counter("ledgermesh.outbox.published");
    this.failures = registry.counter("ledgermesh.outbox.send.failures");
    this.resends = registry.counter("ledgermesh.outbox.resends");
    registry.gauge("ledgermesh.outbox.backlog", repository, r -> r.countByPublishedAtIsNull());
  }

  /** Publishes pending rows. Returns how many rows were confirmed by the broker. */
  public int relayPending() {
    List<OutboxEvent> batch = repository.findTop200ByPublishedAtIsNullOrderByIdAsc();
    int sent = 0;
    for (OutboxEvent row : batch) {
      if (row.getAttemptedAt() != null) {
        resends.increment();
        log.warn(
            "re-sending event {} on {}: the last attempt never confirmed",
            row.getEventId(),
            row.getTopic());
      }
      row.markAttempted(clock.instant());
      repository.saveAndFlush(row);
      if (!send(row)) {
        break;
      }
      row.markPublished(clock.instant());
      repository.save(row);
      sent++;
    }
    return sent;
  }

  private boolean send(OutboxEvent row) {
    ProducerRecord<String, String> record =
        new ProducerRecord<>(row.getTopic(), row.getMessageKey(), row.getPayload());
    record.headers().add(CorrelationId.HEADER, row.getCorrelationId().getBytes());
    record.headers().add("event-id", row.getEventId().getBytes());
    record.headers().add("event-type", row.getEventType().getBytes());
    try {
      kafka.send(record).get(sendTimeoutMs, TimeUnit.MILLISECONDS);
      published.increment();
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (ExecutionException | TimeoutException e) {
      failures.increment();
      log.warn(
          "outbox send failed for event {} on {}: {}",
          row.getEventId(),
          row.getTopic(),
          e.getMessage());
      return false;
    }
  }
}
