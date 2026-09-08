package io.ledgermesh.common.dlq;

import io.ledgermesh.common.metrics.KafkaLagMetrics;
import java.util.Map;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Dead letter operations: depth per topic and replay back onto the source topic. */
@RestController
@RequestMapping("/admin/dlq")
public class DlqAdminController {

  public record ReplayResult(String topic, String dlq, int replayed) {}

  private final DlqReplayer replayer;
  private final ObjectProvider<KafkaLagMetrics> lag;

  public DlqAdminController(DlqReplayer replayer, ObjectProvider<KafkaLagMetrics> lag) {
    this.replayer = replayer;
    this.lag = lag;
  }

  /** Unreplayed dead letters per source topic, as of the last metrics refresh. */
  @GetMapping
  public Map<String, Long> depth() {
    KafkaLagMetrics metrics = lag.getIfAvailable();
    return metrics == null ? Map.of() : metrics.dlqDepth();
  }

  @PostMapping("/{topic}/replay")
  public ReplayResult replay(
      @PathVariable String topic, @RequestParam(defaultValue = "100") int max) {
    int replayed = replayer.replay(topic, max);
    return new ReplayResult(topic, io.ledgermesh.common.events.Topics.dlq(topic), replayed);
  }
}
