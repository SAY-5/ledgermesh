package io.ledgermesh.order.api;

import io.ledgermesh.order.domain.OrderItem;
import io.ledgermesh.order.saga.OrderSagaService;
import jakarta.validation.Valid;
import java.net.URI;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

  private final OrderSagaService saga;

  public OrderController(OrderSagaService saga) {
    this.saga = saga;
  }

  /** Accepts the order and returns 202; the saga completes asynchronously. */
  @PostMapping
  public ResponseEntity<OrderResponse> create(@Valid @RequestBody CreateOrderRequest request) {
    var items =
        request.items().stream()
            .map(i -> new OrderItem(i.sku(), i.quantity(), i.unitPrice()))
            .toList();
    var order = saga.create(request.customerId(), items);
    return ResponseEntity.accepted()
        .location(URI.create("/orders/" + order.getId()))
        .body(OrderResponse.from(order));
  }

  @GetMapping("/{id}")
  public ResponseEntity<OrderResponse> get(@PathVariable String id) {
    return saga.find(id)
        .map(order -> ResponseEntity.ok(OrderResponse.from(order)))
        .orElseGet(() -> ResponseEntity.notFound().build());
  }
}
