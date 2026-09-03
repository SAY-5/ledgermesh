package io.ledgermesh.common.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.UncheckedIOException;

/** JSON encoding of events. Messages are plain JSON strings so any client can read the bus. */
public final class EventCodec {

  private final ObjectMapper mapper;

  public EventCodec(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  public String encode(DomainEvent event) {
    try {
      return mapper.writeValueAsString(event);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  public <T extends DomainEvent> T decode(String json, Class<T> type) {
    try {
      return mapper.readValue(json, type);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("malformed event payload for " + type.getSimpleName(), e);
    }
  }
}
