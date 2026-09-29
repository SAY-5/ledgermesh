package io.ledgermesh.payment.processor;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

/**
 * Deterministic stand-in for a card network. Outcomes depend only on the inputs, so a run is
 * reproducible:
 *
 * <ul>
 *   <li>customers whose id ends in {@code -declined} and amounts above the limit are declined
 *   <li>a configurable share of first attempts fails with a transient fault (retry succeeds)
 *   <li>a smaller share of first attempts hangs past the time limit
 * </ul>
 *
 * <p>Every approval is a new authorization under a code of its own, recorded as outstanding in
 * {@link AuthorizationHolds} before it is returned, and stays outstanding until {@link #release}
 * gives it back, so a run can check that no card is left holding money for an order that was not
 * confirmed, and that a confirmed one is held once. An approval whose caller stopped waiting (the
 * time limiter cut it off) is outstanding all the same, as it would be at a real processor. Which
 * attempts approve, fail or hang depends only on the inputs; the codes do not.
 */
public class SyntheticProcessor implements PaymentProcessor {

  private final BigDecimal limit;
  private final int transientPercent;
  private final int slowPercent;
  private final long slowMillis;
  private final AuthorizationHolds holds;

  public SyntheticProcessor(
      BigDecimal limit,
      int transientPercent,
      int slowPercent,
      long slowMillis,
      AuthorizationHolds holds) {
    this.limit = limit;
    this.transientPercent = transientPercent;
    this.slowPercent = slowPercent;
    this.slowMillis = slowMillis;
    this.holds = holds;
  }

  @Override
  public Result authorize(String orderId, String customerId, BigDecimal amount, int attempt) {
    if (customerId.endsWith("-declined")) {
      return new Declined("CARD_DECLINED");
    }
    if (amount.compareTo(limit) > 0) {
      return new Declined("OVER_LIMIT");
    }
    int bucket = bucket(orderId + ":" + attempt);
    if (bucket < transientPercent) {
      throw new ProcessorUnavailableException("processor busy (attempt " + attempt + ")");
    }
    if (bucket < transientPercent + slowPercent) {
      sleep(slowMillis);
    }
    String code =
        "AUTH-" + UUID.randomUUID().toString().replace("-", "").substring(0, 16).toUpperCase();
    holds.grant(orderId, code, amount);
    return new Approved(code);
  }

  @Override
  public int release(String orderId, String keep) {
    return holds.release(orderId, keep);
  }

  @Override
  public boolean releaseAuthorization(String orderId, String authorizationCode) {
    return holds.releaseCode(orderId, authorizationCode);
  }

  @Override
  public List<Authorization> outstanding(int limit) {
    return holds.outstanding(limit);
  }

  /** A call answers at once or after the slow path's {@code slow-millis}; nothing takes longer. */
  @Override
  public Duration longestCall() {
    return Duration.ofMillis(slowMillis);
  }

  static int bucket(String input) {
    return Integer.parseInt(digest(input).substring(0, 4), 16) % 100;
  }

  private static String digest(String input) {
    try {
      byte[] hash =
          MessageDigest.getInstance("SHA-256").digest(input.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(hash);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ProcessorUnavailableException("interrupted while waiting for processor");
    }
  }
}
