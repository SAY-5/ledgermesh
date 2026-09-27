package io.ledgermesh.common.idempotency;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import java.time.Instant;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import org.springframework.data.domain.Persistable;

/**
 * The answer a client already got for an idempotency key. Written in the same transaction as the
 * effect it describes, so the key and the effect can never disagree.
 */
@Entity
@Table(name = "idempotent_request")
public class IdempotentRequest implements Persistable<String> {

  @Id
  @Column(length = 128)
  private String id;

  @JdbcTypeCode(SqlTypes.LONGVARCHAR)
  @Column(nullable = false)
  private String response;

  @Column(nullable = false)
  private Instant createdAt;

  /**
   * A new answer is inserted, never merged. The key is assigned, so a merge would read the row
   * first and, when a racing request had committed the same key in the meantime, overwrite that
   * answer and let a second effect commit; an insert fails on the primary key instead.
   */
  @Transient private boolean isNew = true;

  protected IdempotentRequest() {}

  public IdempotentRequest(String id, String response, Instant createdAt) {
    this.id = id;
    this.response = response;
    this.createdAt = createdAt;
  }

  @Override
  public String getId() {
    return id;
  }

  public String getResponse() {
    return response;
  }

  public Instant getCreatedAt() {
    return createdAt;
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
