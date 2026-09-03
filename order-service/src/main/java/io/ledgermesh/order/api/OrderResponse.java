package io.ledgermesh.order.api;

import io.ledgermesh.order.domain.Order;
import io.ledgermesh.order.domain.OrderStatus;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public record OrderResponse(
    String id,
    String customerId,
    OrderStatus status,
    String reason,
    BigDecimal amount,
    List<Line> items,
    String correlationId,
    Instant createdAt,
    Instant updatedAt) {

  public record Line(String sku, int quantity, BigDecimal unitPrice) {}

  public static OrderResponse from(Order order) {
    return new OrderResponse(
        order.getId(),
        order.getCustomerId(),
        order.getStatus(),
        order.getReason(),
        order.getAmount(),
        order.getItems().stream()
            .map(i -> new Line(i.getSku(), i.getQuantity(), i.getUnitPrice()))
            .toList(),
        order.getCorrelationId(),
        order.getCreatedAt(),
        order.getUpdatedAt());
  }
}
