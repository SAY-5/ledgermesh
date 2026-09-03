package io.ledgermesh.order.stock;

/** Synchronous call to the inventory service. Kept behind an interface so tests can fail it. */
public interface InventoryGateway {

  StockView fetch(String sku);
}
