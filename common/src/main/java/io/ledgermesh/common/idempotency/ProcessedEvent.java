package io.ledgermesh.common.idempotency;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import java.time.Instant;
import org.springframework.data.domain.Persistable;

/** Record of an event a consumer has already applied. Keyed by consumer name plus event id. */
@Entity
@Table(name = "processed_event")
public class ProcessedEvent implements Persistable<String> {

  @Id
  @Column(length = 160)
  private String id;

  @Column(nullable = false)
  private Instant processedAt;

  /**
   * A new marker is inserted, never merged: a merge would overwrite the marker of a delivery that
   * committed in the meantime and let this one's work commit as well.
   */
  @Transient private boolean isNew = true;

  protected ProcessedEvent() {}

  public ProcessedEvent(String consumer, String eventId, Instant processedAt) {
    this.id = key(consumer, eventId);
    this.processedAt = processedAt;
  }

  public static String key(String consumer, String eventId) {
    return consumer + ":" + eventId;
  }

  @Override
  public String getId() {
    return id;
  }

  public Instant getProcessedAt() {
    return processedAt;
  }

  @Override
  public boolean isNew() {
    return isNew;
  }

  @PostLoad
  @PostPersist
  void markStored() {
    isNew = false;
  }
}
