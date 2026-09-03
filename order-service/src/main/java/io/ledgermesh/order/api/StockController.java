package io.ledgermesh.order.api;

import io.ledgermesh.order.stock.StockCheckClient;
import io.ledgermesh.order.stock.StockView;
import java.util.concurrent.CompletableFuture;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StockController {

  private final StockCheckClient stock;

  public StockController(StockCheckClient stock) {
    this.stock = stock;
  }

  /** Current stock for a sku, live when inventory answers in time, otherwise last known. */
  @GetMapping("/stock/{sku}")
  public CompletableFuture<StockView> stock(@PathVariable String sku) {
    return stock.stock(sku);
  }
}
