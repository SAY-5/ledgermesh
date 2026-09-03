package io.ledgermesh.order.domain;

public enum OrderStatus {
  PENDING,
  RESERVED,
  CONFIRMED,
  CANCELLED;

  public boolean isTerminal() {
    return this == CONFIRMED || this == CANCELLED;
  }
}
