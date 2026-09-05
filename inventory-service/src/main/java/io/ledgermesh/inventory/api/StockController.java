package io.ledgermesh.inventory.api;

import io.ledgermesh.inventory.stock.ReservationService;
import io.ledgermesh.inventory.stock.StockItem;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/stock")
public class StockController {

  public record StockResponse(String sku, int available) {}

  public record RestockRequest(@Min(0) int available) {}

  private final ReservationService reservations;

  public StockController(ReservationService reservations) {
    this.reservations = reservations;
  }

  /** Cache first, database on a miss. */
  @GetMapping("/{sku}")
  public ResponseEntity<StockResponse> get(@PathVariable String sku) {
    return reservations
        .available(sku)
        .map(available -> ResponseEntity.ok(new StockResponse(sku, available)))
        .orElseGet(() -> ResponseEntity.notFound().build());
  }

  /** Sets the available quantity for a sku, creating it when missing. */
  @PutMapping("/{sku}")
  public StockResponse restock(@PathVariable String sku, @Valid @RequestBody RestockRequest body) {
    StockItem item = reservations.restock(sku, body.available());
    return new StockResponse(item.getSku(), item.getAvailable());
  }
}
