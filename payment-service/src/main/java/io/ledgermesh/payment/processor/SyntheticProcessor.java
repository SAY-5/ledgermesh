package io.ledgermesh.payment.processor;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Deterministic stand-in for a card network. Outcomes depend only on the inputs, so a run is
 * reproducible:
 *
 * <ul>
 *   <li>customers whose id ends in {@code -declined} and amounts above the limit are declined
 *   <li>a configurable share of first attempts fails with a transient fault (retry succeeds)
 *   <li>a smaller share of first attempts hangs past the time limit
 * </ul>
 */
@Component
public class SyntheticProcessor implements PaymentProcessor {

  private final BigDecimal limit;
  private final int transientPercent;
  private final int slowPercent;
  private final long slowMillis;

  public SyntheticProcessor(
      @Value("${ledgermesh.processor.limit:10000}") BigDecimal limit,
      @Value("${ledgermesh.processor.transient-percent:5}") int transientPercent,
      @Value("${ledgermesh.processor.slow-percent:1}") int slowPercent,
      @Value("${ledgermesh.processor.slow-millis:3000}") long slowMillis) {
    this.limit = limit;
    this.transientPercent = transientPercent;
    this.slowPercent = slowPercent;
    this.slowMillis = slowMillis;
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
    return new Approved("AUTH-" + digest(orderId).substring(0, 12).toUpperCase());
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
