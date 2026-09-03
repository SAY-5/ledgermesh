package io.ledgermesh.common.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import java.time.Instant;

/**
 * A row written in the same database transaction as the business change it announces. The relay
 * publishes rows in id order and stamps {@code publishedAt} only after the broker acknowledged the
 * write, so a crash anywhere in between results in a re-send, never a loss.
 */
@Entity
@Table(
    name = "outbox_event",
    indexes = @Index(name = "ix_outbox_unpublished", columnList = "publishedAt, id"))
public class OutboxEvent {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(nullable = false, unique = true, length = 64)
  private String eventId;

  @Column(nullable = false, length = 64)
  private String topic;

  @Column(nullable = false, length = 64)
  private String messageKey;

  @Column(nullable = false, length = 64)
  private String eventType;

  @Column(nullable = false, length = 64)
  private String correlationId;

  @Lob
  @Column(nullable = false)
  private String payload;

  @Column(nullable = false)
  private Instant createdAt;

  private Instant publishedAt;

  protected OutboxEvent() {}

  public OutboxEvent(
      String eventId,
      String topic,
      String messageKey,
      String eventType,
      String correlationId,
      String payload,
      Instant createdAt) {
    this.eventId = eventId;
    this.topic = topic;
    this.messageKey = messageKey;
    this.eventType = eventType;
    this.correlationId = correlationId;
    this.payload = payload;
    this.createdAt = createdAt;
  }

  public Long getId() {
    return id;
  }

  public String getEventId() {
    return eventId;
  }

  public String getTopic() {
    return topic;
  }

  public String getMessageKey() {
    return messageKey;
  }

  public String getEventType() {
    return eventType;
  }

  public String getCorrelationId() {
    return correlationId;
  }

  public String getPayload() {
    return payload;
  }

  public Instant getCreatedAt() {
    return createdAt;
  }

  public Instant getPublishedAt() {
    return publishedAt;
  }

  public void markPublished(Instant at) {
    this.publishedAt = at;
  }
}
