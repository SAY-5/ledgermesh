package io.ledgermesh.common.dlq;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

class PoisonMessagePolicyTest {

  private final PoisonMessagePolicy policy = new PoisonMessagePolicy(2);

  @Test
  void readsTheReplayCountFromTheHeader() {
    assertThat(PoisonMessagePolicy.replayCount(headers("3"))).isEqualTo(3);
  }

  @Test
  void countsAMissingOrUnreadableHeaderAsNoReplays() {
    assertThat(PoisonMessagePolicy.replayCount(new RecordHeaders())).isZero();
    assertThat(PoisonMessagePolicy.replayCount(headers("later"))).isZero();
  }

  @Test
  void allowsReplaysUpToTheCap() {
    assertThat(policy.isPoison(new RecordHeaders())).isFalse();
    assertThat(policy.isPoison(headers("1"))).isFalse();
  }

  @Test
  void marksARecordPoisonOnceTheReplaysAreUsedUp() {
    assertThat(policy.isPoison(headers("2"))).isTrue();
    assertThat(policy.isPoison(headers("7"))).isTrue();
  }

  private static Headers headers(String count) {
    return new RecordHeaders()
        .add(PoisonMessagePolicy.REPLAY_COUNT_HEADER, count.getBytes(StandardCharsets.UTF_8));
  }
}
