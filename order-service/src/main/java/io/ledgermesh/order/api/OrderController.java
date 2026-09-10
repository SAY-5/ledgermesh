package io.ledgermesh.order.api;

import io.ledgermesh.common.idempotency.RequestDeduplicator;
import io.ledgermesh.common.idempotency.RequestDeduplicator.Outcome;
import io.ledgermesh.order.domain.OrderEvent;
import io.ledgermesh.order.domain.OrderItem;
import io.ledgermesh.order.domain.OrderStatus;
import io.ledgermesh.order.saga.OrderSagaService;
import jakarta.validation.Valid;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

  public static final String IDEMPOTENCY_KEY = "Idempotency-Key";
  public static final String IDEMPOTENT_REPLAY = "Idempotent-Replay";

  private final OrderSagaService saga;
  private final RequestDeduplicator requests;

  public OrderController(OrderSagaService saga, RequestDeduplicator requests) {
    this.saga = saga;
    this.requests = requests;
  }

  /**
   * Accepts the order and returns 202; the saga completes asynchronously. Repeating the request
   * with the same {@code Idempotency-Key} returns the first answer instead of placing a second
   * order.
   */
  @PostMapping
  public ResponseEntity<OrderResponse> create(
      @RequestHeader(name = IDEMPOTENCY_KEY, required = false) String key,
      @Valid @RequestBody CreateOrderRequest request) {
    if (key == null || key.isBlank()) {
      return accepted(place(request), false);
    }
    try {
      Outcome<OrderResponse> outcome =
          requests.once(key, OrderResponse.class, () -> place(request));
      return accepted(outcome.body(), outcome.replayed());
    } catch (DataIntegrityViolationException raced) {
      return accepted(requests.stored(key, OrderResponse.class).orElseThrow(() -> raced), true);
    }
  }

  private OrderResponse place(CreateOrderRequest request) {
    var items =
        request.items().stream()
            .map(i -> new OrderItem(i.sku(), i.quantity(), i.unitPrice()))
            .toList();
    return OrderResponse.from(saga.create(request.customerId(), items));
  }

  private static ResponseEntity<OrderResponse> accepted(OrderResponse order, boolean replayed) {
    return ResponseEntity.accepted()
        .location(URI.create("/orders/" + order.id()))
        .header(IDEMPOTENT_REPLAY, Boolean.toString(replayed))
        .body(order);
  }

  @GetMapping("/{id}")
  public ResponseEntity<OrderResponse> get(@PathVariable String id) {
    return saga.find(id)
        .map(order -> ResponseEntity.ok(OrderResponse.from(order)))
        .orElseGet(() -> ResponseEntity.notFound().build());
  }

  /** Every saga step the order went through, in order, with timestamps. */
  @GetMapping("/{id}/timeline")
  public ResponseEntity<List<TimelineEntry>> timeline(@PathVariable String id) {
    return saga.timeline(id)
        .map(events -> ResponseEntity.ok(events.stream().map(TimelineEntry::from).toList()))
        .orElseGet(() -> ResponseEntity.notFound().build());
  }

  public record TimelineEntry(
      String type,
      OrderStatus from,
      OrderStatus to,
      String reason,
      String correlationId,
      Instant at) {

    static TimelineEntry from(OrderEvent e) {
      return new TimelineEntry(
          e.getType(),
          e.getFromStatus(),
          e.getToStatus(),
          e.getReason(),
          e.getCorrelationId(),
          e.getOccurredAt());
    }
  }
}
