package io.ledgermesh.common.events;

/** Kafka topic names shared by every service. */
public final class Topics {

  public static final String ORDER_CREATED = "order.created";
  public static final String ORDER_CANCELLED = "order.cancelled";
  public static final String PAYMENT_REQUESTED = "order.payment_requested";
  public static final String INVENTORY_RESERVED = "inventory.reserved";
  public static final String INVENTORY_REJECTED = "inventory.rejected";
  public static final String PAYMENT_COMPLETED = "payment.completed";
  public static final String PAYMENT_FAILED = "payment.failed";

  public static final String[] ALL = {
    ORDER_CREATED,
    ORDER_CANCELLED,
    PAYMENT_REQUESTED,
    INVENTORY_RESERVED,
    INVENTORY_REJECTED,
    PAYMENT_COMPLETED,
    PAYMENT_FAILED
  };

  /** Suffix of the dead letter topic that shadows every business topic. */
  public static final String DLQ_SUFFIX = ".dlq";

  /** Suffix of the terminal topic that keeps dead letters whose replays are used up. */
  public static final String PARKED_SUFFIX = ".parked";

  private Topics() {}

  public static String dlq(String topic) {
    return sourceOf(topic) + DLQ_SUFFIX;
  }

  public static String parked(String topic) {
    return sourceOf(topic) + PARKED_SUFFIX;
  }

  public static boolean isDlq(String topic) {
    return topic.endsWith(DLQ_SUFFIX);
  }

  public static boolean isParked(String topic) {
    return topic.endsWith(PARKED_SUFFIX);
  }

  /** The business topic behind a dead letter or parked topic; a business topic maps to itself. */
  public static String sourceOf(String topic) {
    if (isDlq(topic)) {
      return topic.substring(0, topic.length() - DLQ_SUFFIX.length());
    }
    if (isParked(topic)) {
      return topic.substring(0, topic.length() - PARKED_SUFFIX.length());
    }
    return topic;
  }
}
