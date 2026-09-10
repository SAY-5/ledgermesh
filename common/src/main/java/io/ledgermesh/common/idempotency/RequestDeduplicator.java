package io.ledgermesh.common.idempotency;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.util.Optional;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Runs an inbound request once per idempotency key. The stored answer and the work share one
 * transaction, so a client that retries a request it never saw the answer to gets the first answer
 * back instead of a second effect. Two requests racing on one key both try to store the answer; the
 * primary key lets exactly one of them through and the loser reads what the winner wrote.
 */
@Component
public class RequestDeduplicator {

  /** The body to answer with, and whether it came from the store rather than from fresh work. */
  public record Outcome<T>(T body, boolean replayed) {}

  private static final Logger log = LoggerFactory.getLogger(RequestDeduplicator.class);

  private final IdempotentRequestRepository requests;
  private final ObjectMapper json;
  private final Clock clock;
  private final Counter replays;

  public RequestDeduplicator(
      IdempotentRequestRepository requests, ObjectMapper json, Clock clock, MeterRegistry meters) {
    this.requests = requests;
    this.json = json;
    this.clock = clock;
    this.replays = meters.counter("ledgermesh.requests.replayed");
  }

  @Transactional
  public <T> Outcome<T> once(String key, Class<T> type, Supplier<T> work) {
    Optional<IdempotentRequest> seen = requests.findById(key);
    if (seen.isPresent()) {
      replays.increment();
      log.info("request {} answered from the idempotency store", key);
      return new Outcome<>(decode(seen.get().getResponse(), type), true);
    }
    T body = work.get();
    requests.saveAndFlush(new IdempotentRequest(key, encode(body), clock.instant()));
    return new Outcome<>(body, false);
  }

  /** The stored answer for a key, for a caller that lost the race to write it. */
  @Transactional(readOnly = true)
  public <T> Optional<T> stored(String key, Class<T> type) {
    return requests.findById(key).map(request -> decode(request.getResponse(), type));
  }

  private String encode(Object body) {
    try {
      return json.writeValueAsString(body);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  private <T> T decode(String stored, Class<T> type) {
    try {
      return json.readValue(stored, type);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(
          "stored response is not readable as " + type.getSimpleName(), e);
    }
  }
}
