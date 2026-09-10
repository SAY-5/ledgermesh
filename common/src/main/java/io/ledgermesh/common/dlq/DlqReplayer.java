package io.ledgermesh.common.dlq;

import io.ledgermesh.common.events.Topics;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;

/**
 * Moves dead letters back onto their source topic. Records are read from {@code <topic>.dlq} with a
 * dedicated consumer group, republished with their original key, value and business headers, and
 * only then committed, so a crash mid replay repeats a record rather than dropping it. Every
 * consumer of the source topic sees the replay; the ones that had already applied the event ignore
 * it by event id. A record that has used up its replays is parked instead of republished, which is
 * what ends the loop between a topic and its dead letter shadow.
 */
public class DlqReplayer {

  /** Outcome of one replay run: records put back on the topic, and records left behind. */
  public record Replayed(int replayed, int parked) {}

  public static final String REPLAYED_AT_HEADER = "dlq-replayed-at";
  public static final String GROUP_SUFFIX = "-dlq-replay";

  private static final Logger log = LoggerFactory.getLogger(DlqReplayer.class);
  private static final Duration POLL = Duration.ofMillis(500);
  private static final Duration IDLE = Duration.ofSeconds(3);

  private final ConsumerFactory<String, String> consumers;
  private final KafkaTemplate<String, String> kafka;
  private final String group;
  private final Clock clock;
  private final MeterRegistry meters;
  private final PoisonMessagePolicy policy;

  public DlqReplayer(
      ConsumerFactory<String, String> consumers,
      KafkaTemplate<String, String> kafka,
      String serviceName,
      Clock clock,
      MeterRegistry meters,
      PoisonMessagePolicy policy) {
    this.consumers = consumers;
    this.kafka = kafka;
    this.group = serviceName + GROUP_SUFFIX;
    this.clock = clock;
    this.meters = meters;
    this.policy = policy;
  }

  public String group() {
    return group;
  }

  /** Handles up to {@code max} dead letters of {@code topic}, replaying or parking each. */
  public Replayed replay(String topic, int max) {
    String dlq = Topics.dlq(Topics.sourceOf(topic));
    Properties overrides = new Properties();
    overrides.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(Math.max(1, max)));
    overrides.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    overrides.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    int replayed = 0;
    int parked = 0;
    try (Consumer<String, String> consumer =
        consumers.createConsumer(group, null, "replay", overrides)) {
      consumer.subscribe(List.of(dlq));
      Instant idleSince = clock.instant();
      while (replayed + parked < max) {
        ConsumerRecords<String, String> records = consumer.poll(POLL);
        if (records.isEmpty()) {
          if (!consumer.assignment().isEmpty()
              && Duration.between(idleSince, clock.instant()).compareTo(IDLE) >= 0) {
            break;
          }
          continue;
        }
        idleSince = clock.instant();
        for (ConsumerRecord<String, String> record : records) {
          if (replayed + parked >= max) {
            break;
          }
          if (policy.isPoison(record.headers())) {
            park(record);
            parked++;
          } else {
            republish(record);
            replayed++;
          }
        }
        consumer.commitSync();
      }
    }
    log.info(
        "replayed {} and parked {} dead letter(s) from {} to {}",
        replayed,
        parked,
        dlq,
        Topics.sourceOf(dlq));
    return new Replayed(replayed, parked);
  }

  private void park(ConsumerRecord<String, String> record) {
    String target = Topics.sourceOf(record.topic());
    meters.counter("ledgermesh.dlq.parked", "topic", target).increment();
    log.warn(
        "dead letter {}-{}@{} parked after {} replay(s)",
        record.topic(),
        record.partition(),
        record.offset(),
        PoisonMessagePolicy.replayCount(record.headers()));
  }

  private void republish(ConsumerRecord<String, String> record) {
    String target = Topics.sourceOf(record.topic());
    int count = PoisonMessagePolicy.replayCount(record.headers());
    ProducerRecord<String, String> out = new ProducerRecord<>(target, record.key(), record.value());
    for (Header header : record.headers()) {
      if (!header.key().startsWith(KafkaHeaders.PREFIX)
          && !header.key().equals(PoisonMessagePolicy.REPLAY_COUNT_HEADER)
          && !header.key().equals(REPLAYED_AT_HEADER)) {
        out.headers().add(header);
      }
    }
    out.headers()
        .add(
            PoisonMessagePolicy.REPLAY_COUNT_HEADER,
            Integer.toString(count + 1).getBytes(StandardCharsets.UTF_8));
    out.headers()
        .add(REPLAYED_AT_HEADER, clock.instant().toString().getBytes(StandardCharsets.UTF_8));
    try {
      kafka.send(out).get(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while replaying " + record.topic(), e);
    } catch (ExecutionException | TimeoutException e) {
      throw new IllegalStateException("replay to " + target + " failed", e);
    }
    meters.counter("ledgermesh.dlq.replayed", "topic", target).increment();
  }
}
