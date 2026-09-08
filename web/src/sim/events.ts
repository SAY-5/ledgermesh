/** Event contracts and topic names, mirrored from the common module. */

export const Topics = {
  ORDER_CREATED: "order.created",
  ORDER_CANCELLED: "order.cancelled",
  INVENTORY_RESERVED: "inventory.reserved",
  INVENTORY_REJECTED: "inventory.rejected",
  PAYMENT_COMPLETED: "payment.completed",
  PAYMENT_FAILED: "payment.failed",
} as const;

export type Topic = (typeof Topics)[keyof typeof Topics];

export const ALL_TOPICS: Topic[] = Object.values(Topics);

export interface OrderLine {
  sku: string;
  quantity: number;
}

interface Base {
  eventId: string;
  orderId: string;
  correlationId: string;
  occurredAt: number;
}

export interface OrderCreated extends Base {
  type: "OrderCreated";
  topic: typeof Topics.ORDER_CREATED;
  customerId: string;
  lines: OrderLine[];
  amount: number;
}

export interface OrderCancelled extends Base {
  type: "OrderCancelled";
  topic: typeof Topics.ORDER_CANCELLED;
  reason: string;
  lines: OrderLine[];
}

export interface InventoryReserved extends Base {
  type: "InventoryReserved";
  topic: typeof Topics.INVENTORY_RESERVED;
  customerId: string;
  lines: OrderLine[];
  amount: number;
}

export interface InventoryRejected extends Base {
  type: "InventoryRejected";
  topic: typeof Topics.INVENTORY_REJECTED;
  reason: string;
}

export interface PaymentCompleted extends Base {
  type: "PaymentCompleted";
  topic: typeof Topics.PAYMENT_COMPLETED;
  authorizationCode: string;
}

export interface PaymentFailed extends Base {
  type: "PaymentFailed";
  topic: typeof Topics.PAYMENT_FAILED;
  reason: string;
}

export type DomainEvent =
  | OrderCreated
  | OrderCancelled
  | InventoryReserved
  | InventoryRejected
  | PaymentCompleted
  | PaymentFailed;

export type ServiceName = "order-service" | "inventory-service" | "payment-service";

export type TraceSource = ServiceName | "broker" | "redis" | "client" | "chaos";

export interface Trace {
  t: number;
  source: TraceSource;
  kind: string;
  text: string;
  orderId?: string;
}
