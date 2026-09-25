package io.ledgermesh.common.dlq;

import io.ledgermesh.common.events.Topics;
import io.ledgermesh.common.metrics.KafkaLagMetrics;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Dead letter operations: depth per topic, replay back onto the source topic, and the records that
 * were parked once their replays ran out.
 */
@RestController
@RequestMapping("/admin/dlq")
public class DlqAdminController {

  public record ReplayResult(String topic, String dlq, int replayed, int parked) {}

  private final DlqReplayer replayer;
  private final ObjectProvider<KafkaLagMetrics> lag;

  public DlqAdminController(DlqReplayer replayer, ObjectProvider<KafkaLagMetrics> lag) {
    this.replayer = replayer;
    this.lag = lag;
  }

  /** Dead letters per source topic that this service has not replayed or parked yet. */
  @GetMapping
  public Map<String, Long> depth() {
    KafkaLagMetrics metrics = lag.getIfAvailable();
    return metrics == null ? Map.of() : metrics.dlqDepth();
  }

  /** Records parked on {@code <topic>.parked}, per source topic, oldest first. */
  @GetMapping("/parked")
  public Map<String, List<DlqReplayer.ParkedRecord>> parked(
      @RequestParam(defaultValue = "100") int max) {
    return replayer.parked(max);
  }

  @PostMapping("/{topic}/replay")
  public ReplayResult replay(
      @PathVariable String topic, @RequestParam(defaultValue = "100") int max) {
    DlqReplayer.Replayed outcome = replayer.replay(topic, max);
    return new ReplayResult(topic, Topics.dlq(topic), outcome.replayed(), outcome.parked());
  }
}
