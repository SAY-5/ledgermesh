package io.ledgermesh.inventory.stock;

import io.ledgermesh.common.events.OrderLine;
import io.ledgermesh.inventory.cache.StockCache;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Reserves or releases stock for a whole order atomically: every line is locked, checked, and
 * only then decremented, so an order never ends up half reserved. Each change is written through
 * to the cache once the transaction commits.
 */
@Service
public class ReservationService {

  public sealed interface Outcome permits Reserved, Rejected {}

  public record Reserved(Map<String, Integer> remaining) implements Outcome {}

  public record Rejected(String reason) implements Outcome {}

  private final StockRepository stock;
  private final StockCache cache;

  public ReservationService(StockRepository stock, StockCache cache) {
    this.stock = stock;
    this.cache = cache;
  }

  @Transactional
  public Outcome reserve(List<OrderLine> lines) {
    Map<String, Integer> wanted = merge(lines);
    List<StockItem> items = stock.lockAllBySku(wanted.keySet());
    if (items.size() != wanted.size()) {
      return new Rejected("UNKNOWN_SKU");
    }
    for (StockItem item : items) {
      if (!item.canReserve(wanted.get(item.getSku()))) {
        return new Rejected("OUT_OF_STOCK");
      }
    }
    Map<String, Integer> remaining = new HashMap<>();
    for (StockItem item : items) {
      item.reserve(wanted.get(item.getSku()));
      remaining.put(item.getSku(), item.getAvailable());
      cache.writeThrough(item.getSku(), item.getAvailable());
    }
    return new Reserved(remaining);
  }

  @Transactional
  public void release(List<OrderLine> lines) {
    Map<String, Integer> wanted = merge(lines);
    for (StockItem item : stock.lockAllBySku(wanted.keySet())) {
      item.release(wanted.get(item.getSku()));
      cache.writeThrough(item.getSku(), item.getAvailable());
    }
  }

  @Transactional
  public StockItem restock(String sku, int available) {
    StockItem item = stock.findById(sku).orElseGet(() -> new StockItem(sku, 0));
    item.restock(available);
    StockItem saved = stock.save(item);
    cache.writeThrough(sku, available);
    return saved;
  }

  @Transactional(readOnly = true)
  public Optional<Integer> available(String sku) {
    Optional<Integer> cached = cache.read(sku);
    if (cached.isPresent()) {
      return cached;
    }
    Optional<Integer> fromDb = stock.findById(sku).map(StockItem::getAvailable);
    fromDb.ifPresent(v -> cache.writeThrough(sku, v));
    return fromDb;
  }

  private static Map<String, Integer> merge(List<OrderLine> lines) {
    Map<String, Integer> wanted = new HashMap<>();
    for (OrderLine line : lines) {
      wanted.merge(line.sku(), line.quantity(), Integer::sum);
    }
    return wanted;
  }
}
