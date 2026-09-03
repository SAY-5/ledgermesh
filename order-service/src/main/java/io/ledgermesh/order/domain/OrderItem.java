package io.ledgermesh.order.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import java.math.BigDecimal;

@Embeddable
public class OrderItem {

  @Column(nullable = false, length = 64)
  private String sku;

  @Column(nullable = false)
  private int quantity;

  @Column(nullable = false, precision = 12, scale = 2)
  private BigDecimal unitPrice;

  protected OrderItem() {}

  public OrderItem(String sku, int quantity, BigDecimal unitPrice) {
    this.sku = sku;
    this.quantity = quantity;
    this.unitPrice = unitPrice;
  }

  public String getSku() {
    return sku;
  }

  public int getQuantity() {
    return quantity;
  }

  public BigDecimal getUnitPrice() {
    return unitPrice;
  }

  public BigDecimal lineTotal() {
    return unitPrice.multiply(BigDecimal.valueOf(quantity));
  }
}
