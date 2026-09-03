package io.ledgermesh.common.outbox;

import io.ledgermesh.common.events.DomainEvent;
import io.ledgermesh.common.events.EventCodec;
import java.time.Clock;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/** Appends an event to the outbox. Must be called inside the caller's transaction. */
@Component
public class OutboxWriter {

  private final OutboxEventRepository repository;
  private final EventCodec codec;
  private final Clock clock;

  public OutboxWriter(OutboxEventRepository repository, EventCodec codec, Clock clock) {
    this.repository = repository;
    this.codec = codec;
    this.clock = clock;
  }

  @Transactional(propagation = Propagation.MANDATORY)
  public OutboxEvent append(DomainEvent event) {
    return repository.save(
        new OutboxEvent(
            event.eventId(),
            event.topic(),
            event.orderId(),
            event.getClass().getSimpleName(),
            event.correlationId(),
            codec.encode(event),
            clock.instant()));
  }
}
