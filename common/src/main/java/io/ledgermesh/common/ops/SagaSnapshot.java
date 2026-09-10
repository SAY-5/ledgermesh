package io.ledgermesh.common.ops;

import java.util.Map;

/** The saga numbers a service adds to its own ops overview. Only the order service has them. */
public interface SagaSnapshot {

  record Sagas(long inFlight, long stuck, Map<String, Long> byState) {}

  Sagas sagas();
}
