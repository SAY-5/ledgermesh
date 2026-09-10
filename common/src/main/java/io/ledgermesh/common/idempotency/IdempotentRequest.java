package io.ledgermesh.common.idempotency;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * The answer a client already got for an idempotency key. Written in the same transaction as the
 * effect it describes, so the key and the effect can never disagree.
 */
@Entity
@Table(name = "idempotent_request")
public class IdempotentRequest {

  @Id
  @Column(length = 128)
  private String id;

  @JdbcTypeCode(SqlTypes.LONGVARCHAR)
  @Column(nullable = false)
  private String response;

  @Column(nullable = false)
  private Instant createdAt;

  protected IdempotentRequest() {}

  public IdempotentRequest(String id, String response, Instant createdAt) {
    this.id = id;
    this.response = response;
    this.createdAt = createdAt;
  }

  public String getId() {
    return id;
  }

  public String getResponse() {
    return response;
  }

  public Instant getCreatedAt() {
    return createdAt;
  }
}
