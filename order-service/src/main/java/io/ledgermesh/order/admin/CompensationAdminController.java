package io.ledgermesh.order.admin;

import io.ledgermesh.order.saga.OrderSagaService;
import java.util.ArrayList;
import java.util.List;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Repairs cancellations the payment service never answered. The reaper sends {@code
 * order.cancelled} again for every cancellation that is waiting for an answer, but an order
 * cancelled before the order service waited for answers is not waiting for one; this sends its
 * cancellation again and makes it wait, which is the step to run after upgrading, repeated until it
 * resends nothing. An order it sent is waiting from then on, so each call takes the next ones.
 */
@RestController
@RequestMapping("/admin/compensations")
public class CompensationAdminController {

  public record Resent(int resent, List<String> orders) {}

  static final int MAX_BATCH = 1000;

  private final OrderSagaService saga;

  public CompensationAdminController(OrderSagaService saga) {
    this.saga = saga;
  }

  /**
   * Sends the cancellation of up to {@code max} (1 to 1000) unanswered cancelled orders that are
   * not waiting for an answer again.
   */
  @PostMapping("/resend")
  public Resent resend(@RequestParam(defaultValue = "100") int max) {
    List<String> resent = new ArrayList<>();
    for (String orderId : saga.unansweredCancellations(Math.max(1, Math.min(MAX_BATCH, max)))) {
      if (saga.resendCancellation(orderId)) {
        resent.add(orderId);
      }
    }
    return new Resent(resent.size(), resent);
  }
}
