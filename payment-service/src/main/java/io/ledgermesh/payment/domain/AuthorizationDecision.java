package io.ledgermesh.payment.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

/** Durable, order-bound arbitration between keeping and releasing one processor authorization. */
@Entity
@Table(name = "authorization_decision")
public class AuthorizationDecision {
  public enum State {
    UNDECIDED,
    CLAIMED,
    RETIRED
  }

  @Id
  @Column(length = 64)
  private String authorizationCode;

  // No foreign key: an orphan hold still needs a permanent retirement tombstone.
  @Column(nullable = false, updatable = false, length = 36)
  private String orderId;

  @Enumerated(EnumType.STRING)
  @Column(nullable = false, length = 16)
  private State state;

  // Nullable until inserted: Spring Data must persist, never merge, an absent assigned key.
  @Version
  @Column(nullable = false)
  private Long version;

  protected AuthorizationDecision() {}

  public AuthorizationDecision(String authorizationCode, String orderId) {
    this.authorizationCode = authorizationCode;
    this.orderId = orderId;
    this.state = State.UNDECIDED;
  }

  public void requireOrder(String orderId) {
    if (!this.orderId.equals(orderId)) {
      throw new IllegalArgumentException("Authorization belongs to a different order");
    }
  }

  public boolean isRetired() {
    return state == State.RETIRED;
  }

  public void claim() {
    if (isRetired()) {
      throw new IllegalStateException("Retired authorization cannot be claimed");
    }
    state = State.CLAIMED;
  }

  public void retire() {
    state = State.RETIRED;
  }
}
