package io.ledgermesh.inventory.stock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/** Creates the configured skus on first start so a fresh stack has stock to sell. */
@Component
public class StockSeeder implements ApplicationRunner {

  private static final Logger log = LoggerFactory.getLogger(StockSeeder.class);

  private final StockRepository stock;
  private final ReservationService reservations;
  private final String seed;

  public StockSeeder(
      StockRepository stock,
      ReservationService reservations,
      @Value("${ledgermesh.inventory.seed:}") String seed) {
    this.stock = stock;
    this.reservations = reservations;
    this.seed = seed;
  }

  @Override
  public void run(org.springframework.boot.ApplicationArguments args) {
    if (seed.isBlank()) {
      return;
    }
    for (String entry : seed.split(",")) {
      String[] parts = entry.trim().split("=");
      if (parts.length != 2) {
        continue;
      }
      String sku = parts[0].trim();
      if (stock.existsById(sku)) {
        continue;
      }
      reservations.restock(sku, Integer.parseInt(parts[1].trim()));
      log.info("seeded {} with {} units", sku, parts[1].trim());
    }
  }
}
