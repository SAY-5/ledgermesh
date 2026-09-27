package io.ledgermesh.inventory.stock;

import io.ledgermesh.common.events.OrderLine;
import io.ledgermesh.inventory.cache.StockCache;
import io.ledgermesh.inventory.stock.Reservation.State;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Reserves or releases stock for a whole order atomically: every line is locked, checked, and only
 * then decremented, so an order never ends up half reserved. Every hold is recorded per order and
 * sku in the same transaction, so a release credits exactly what this order took, a release for an
 * order that never reserved is a no-op that blocks a later reservation, and a reservation that is
 * delivered twice under different event ids takes stock once. Each change is written through to the
 * cache once the transaction commits.
 */
@Service
public class ReservationService {

  public sealed interface Outcome permits Reserved, Rejected {}

  public record Reserved(Map<String, Integer> remaining) implements Outcome {}

  public record Rejected(String reason) implements Outcome {}

  public static final String ALREADY_RELEASED = "ALREADY_RELEASED";

  private final StockRepository stock;
  private final ReservationRepository reservations;
  private final StockCache cache;
  private final Clock clock;

  public ReservationService(
      StockRepository stock, ReservationRepository reservations, StockCache cache, Clock clock) {
    this.stock = stock;
    this.reservations = reservations;
    this.cache = cache;
    this.clock = clock;
  }

  @Transactional
  public Outcome reserve(String orderId, List<OrderLine> lines) {
    Map<String, Integer> wanted = merge(lines);
    List<StockItem> items = stock.lockAllBySku(wanted.keySet());
    if (items.size() != wanted.size()) {
      return new Rejected("UNKNOWN_SKU");
    }
    // the stock rows are locked, so the ledger read below cannot race a release for this order
    List<Reservation> ledger = reservations.findByOrderIdOrderBySkuAsc(orderId);
    if (!ledger.isEmpty()) {
      if (ledger.stream().anyMatch(r -> r.getState() == State.RELEASED)) {
        return new Rejected(ALREADY_RELEASED);
      }
      return new Reserved(remaining(items));
    }
    for (StockItem item : items) {
      if (!item.canReserve(wanted.get(item.getSku()))) {
        return new Rejected("OUT_OF_STOCK");
      }
    }
    for (StockItem item : items) {
      int quantity = wanted.get(item.getSku());
      item.reserve(quantity);
      reservations.save(Reservation.reserved(orderId, item.getSku(), quantity, clock.instant()));
      cache.writeThrough(item.getSku(), item.getAvailable());
    }
    return new Reserved(remaining(items));
  }

  /**
   * Returns the units this order is holding to stock. Credits what the ledger recorded, not what
   * the event says; returns how many units were released, zero when the order held nothing.
   */
  @Transactional
  public int release(String orderId, List<OrderLine> lines) {
    TreeSet<String> skus = new TreeSet<>(merge(lines).keySet());
    for (Reservation row : reservations.findByOrderIdOrderBySkuAsc(orderId)) {
      skus.add(row.getSku());
    }
    Map<String, StockItem> items = new HashMap<>();
    for (StockItem item : stock.lockAllBySku(skus)) {
      items.put(item.getSku(), item);
    }
    List<Reservation> ledger = reservations.findByOrderIdOrderBySkuAsc(orderId);
    if (ledger.isEmpty()) {
      // nothing to give back; the marker makes a late order.created for this order a no-op too
      for (String sku : skus) {
        reservations.save(Reservation.releasedMarker(orderId, sku, clock.instant()));
      }
      return 0;
    }
    int released = 0;
    for (Reservation row : ledger) {
      StockItem item = items.get(row.getSku());
      if (row.getState() != State.RESERVED || item == null) {
        continue;
      }
      item.release(row.getQuantity());
      row.release(clock.instant());
      released += row.getQuantity();
      cache.writeThrough(item.getSku(), item.getAvailable());
    }
    return released;
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

  private static Map<String, Integer> remaining(List<StockItem> items) {
    Map<String, Integer> remaining = new HashMap<>();
    for (StockItem item : items) {
      remaining.put(item.getSku(), item.getAvailable());
    }
    return remaining;
  }

  private static Map<String, Integer> merge(List<OrderLine> lines) {
    Map<String, Integer> wanted = new HashMap<>();
    for (OrderLine line : lines) {
      wanted.merge(line.sku(), line.quantity(), Integer::sum);
    }
    return wanted;
  }
}
