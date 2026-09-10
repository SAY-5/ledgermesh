package io.ledgermesh.common.dlq;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

/**
 * Caps how often one dead letter may be put back on its source topic. A record that fails, is
 * replayed and fails again would otherwise cycle between the topic and its dead letter shadow for
 * ever, burning the whole retry ladder on every lap. The count travels with the record in a header,
 * so the cap holds across a restart of the service that replays.
 */
public final class PoisonMessagePolicy {

  public static final String REPLAY_COUNT_HEADER = "dlq-replay-count";

  private final int maxReplays;

  public PoisonMessagePolicy(int maxReplays) {
    this.maxReplays = maxReplays;
  }

  public int maxReplays() {
    return maxReplays;
  }

  /** True when the record has used up its replays and must stay in the dead letter topic. */
  public boolean isPoison(Headers headers) {
    return replayCount(headers) >= maxReplays;
  }

  /** Replays this record has already had. An absent or unreadable header counts as none. */
  public static int replayCount(Headers headers) {
    Header header = headers.lastHeader(REPLAY_COUNT_HEADER);
    if (header == null || header.value() == null) {
      return 0;
    }
    try {
      return Integer.parseInt(new String(header.value(), StandardCharsets.UTF_8).trim());
    } catch (NumberFormatException e) {
      return 0;
    }
  }
}
