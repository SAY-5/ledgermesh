package io.ledgermesh.order;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;

/** A clock the tests can move forward, so deadlines expire without sleeping. */
public final class TestClock extends Clock {

  private final AtomicReference<Instant> now =
      new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z"));

  public void advance(Duration by) {
    now.updateAndGet(t -> t.plus(by));
  }

  @Override
  public ZoneId getZone() {
    return ZoneOffset.UTC;
  }

  @Override
  public Clock withZone(ZoneId zone) {
    return this;
  }

  @Override
  public Instant instant() {
    return now.get();
  }
}
