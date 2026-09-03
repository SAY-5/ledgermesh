package io.ledgermesh.common.idempotency;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

/** Record of an event a consumer has already applied. Keyed by consumer name plus event id. */
@Entity
@Table(name = "processed_event")
public class ProcessedEvent {

  @Id
  @Column(length = 160)
  private String id;

  @Column(nullable = false)
  private Instant processedAt;

  protected ProcessedEvent() {}

  public ProcessedEvent(String consumer, String eventId, Instant processedAt) {
    this.id = key(consumer, eventId);
    this.processedAt = processedAt;
  }

  public static String key(String consumer, String eventId) {
    return consumer + ":" + eventId;
  }

  public String getId() {
    return id;
  }

  public Instant getProcessedAt() {
    return processedAt;
  }
}
