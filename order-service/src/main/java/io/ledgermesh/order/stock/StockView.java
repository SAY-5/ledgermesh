package io.ledgermesh.order.stock;

/** Stock level as seen by the order service. {@code source} tells the caller how fresh it is. */
public record StockView(String sku, Integer available, String source) {

  public static final String LIVE = "live";
  public static final String CACHE = "cache";
  public static final String UNKNOWN = "unknown";

  public static StockView live(String sku, int available) {
    return new StockView(sku, available, LIVE);
  }

  public StockView asCached() {
    return new StockView(sku, available, CACHE);
  }

  public static StockView unknown(String sku) {
    return new StockView(sku, null, UNKNOWN);
  }
}
