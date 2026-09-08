export type OrderStatus = "PENDING" | "RESERVED" | "CONFIRMED" | "CANCELLED";

export type SagaEvent =
  | "INVENTORY_RESERVED"
  | "INVENTORY_REJECTED"
  | "PAYMENT_COMPLETED"
  | "PAYMENT_FAILED";

export const OUT_OF_STOCK = "OUT_OF_STOCK";
export const PAYMENT_DECLINED = "PAYMENT_DECLINED";

export interface Transition {
  to: OrderStatus;
  reason: string | null;
  releaseInventory: boolean;
}

export function isTerminal(status: OrderStatus): boolean {
  return status === "CONFIRMED" || status === "CANCELLED";
}

/**
 * Pure transition table for the order saga.
 *
 *   PENDING  + INVENTORY_RESERVED -> RESERVED
 *   PENDING  + INVENTORY_REJECTED -> CANCELLED (OUT_OF_STOCK)
 *   RESERVED + PAYMENT_COMPLETED  -> CONFIRMED
 *   RESERVED + PAYMENT_FAILED     -> CANCELLED (PAYMENT_DECLINED), release the reservation
 *
 * Payment outcomes are accepted from PENDING too since the two topics are independent and a
 * payment can only exist for a reserved order. Anything on a terminal order is ignored.
 */
export function applyTransition(current: OrderStatus, event: SagaEvent): Transition | null {
  if (isTerminal(current)) return null;
  switch (event) {
    case "INVENTORY_RESERVED":
      return current === "PENDING" ? { to: "RESERVED", reason: null, releaseInventory: false } : null;
    case "INVENTORY_REJECTED":
      return current === "PENDING"
        ? { to: "CANCELLED", reason: OUT_OF_STOCK, releaseInventory: false }
        : null;
    case "PAYMENT_COMPLETED":
      return { to: "CONFIRMED", reason: null, releaseInventory: false };
    case "PAYMENT_FAILED":
      return { to: "CANCELLED", reason: PAYMENT_DECLINED, releaseInventory: true };
  }
}
