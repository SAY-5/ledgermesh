package io.ledgermesh.common.events;

/** A single line of an order: a stock keeping unit and the quantity requested. */
public record OrderLine(String sku, int quantity) {}
