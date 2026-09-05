package io.ledgermesh.inventory.stock;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

@Entity
@Table(name = "stock_item")
public class StockItem {

  @Id
  @Column(length = 64)
  private String sku;

  @Column(nullable = false)
  private int available;

  @Column(nullable = false)
  private int reserved;

  @Version private long version;

  protected StockItem() {}

  public StockItem(String sku, int available) {
    this.sku = sku;
    this.available = available;
    this.reserved = 0;
  }

  public boolean canReserve(int quantity) {
    return available >= quantity;
  }

  public void reserve(int quantity) {
    if (!canReserve(quantity)) {
      throw new IllegalStateException("insufficient stock for " + sku);
    }
    available -= quantity;
    reserved += quantity;
  }

  public void release(int quantity) {
    int amount = Math.min(quantity, reserved);
    reserved -= amount;
    available += amount;
  }

  public void restock(int newAvailable) {
    this.available = newAvailable;
  }

  public String getSku() {
    return sku;
  }

  public int getAvailable() {
    return available;
  }

  public int getReserved() {
    return reserved;
  }
}
