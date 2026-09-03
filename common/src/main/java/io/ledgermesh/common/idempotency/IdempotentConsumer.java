package io.ledgermesh.common.idempotency;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Runs a unit of work exactly once per event id. The processed marker is written in the same
 * transaction as the work, so a crash after commit makes the redelivery a no-op and a crash before
 * commit leaves nothing behind. A primary key violation on the marker is treated as "already
 * done".
 */
@Component
public class IdempotentConsumer {

  private static final Logger log = LoggerFactory.getLogger(IdempotentConsumer.class);

  private final ProcessedEventRepository repository;
  private final Clock clock;
  private final Counter duplicates;

  public IdempotentConsumer(
      ProcessedEventRepository repository, Clock clock, MeterRegistry registry) {
    this.repository = repository;
    this.clock = clock;
    this.duplicates = registry.counter("ledgermesh.consumer.duplicates");
  }

  /** Returns true when the work ran, false when the event had already been processed. */
  @Transactional
  public boolean once(String consumer, String eventId, Runnable work) {
    String key = ProcessedEvent.key(consumer, eventId);
    if (repository.existsById(key)) {
      duplicates.increment();
      log.info("duplicate event {} ignored by {}", eventId, consumer);
      return false;
    }
    work.run();
    repository.saveAndFlush(new ProcessedEvent(consumer, eventId, clock.instant()));
    return true;
  }
}
