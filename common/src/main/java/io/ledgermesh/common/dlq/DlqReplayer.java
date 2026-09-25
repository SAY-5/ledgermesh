package io.ledgermesh.common.dlq;

import io.ledgermesh.common.events.Topics;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;

/**
 * Moves dead letters back onto their source topic. Records are read from {@code <topic>.dlq} with a
 * dedicated consumer group, republished with their original key, value and business headers, and
 * only the offsets of the records actually handled are committed, so a crash mid replay repeats a
 * record rather than dropping it and a budget that ends mid batch leaves the rest in place. Every
 * consumer of the source topic sees the replay; the ones that had already applied the event ignore
 * it by event id. A record that has used up its replays is parked instead of republished: it is
 * copied to {@code <topic>.parked} with every header it carried, which ends the loop between a
 * topic and its dead letter shadow without losing the record from view.
 */
public class DlqReplayer {

  /** Outcome of one replay run: records put back on the topic, and records parked. */
  public record Replayed(int replayed, int parked) {}

  /** A dead letter that used up its replays, as retained on {@code <topic>.parked}. */
  public record ParkedRecord(
      String topic,
      int partition,
      long offset,
      String key,
      int replays,
      Instant parkedAt,
      String originalTopic,
      Integer originalPartition,
      Long originalOffset,
      String exception,
      String message) {}

  public static final String REPLAYED_AT_HEADER = "dlq-replayed-at";
  public static final String PARKED_AT_HEADER = "dlq-parked-at";
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
    String dlq = Topics.dlq(topic);
    Properties overrides = new Properties();
    overrides.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(Math.max(1, max)));
    overrides.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    overrides.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    int replayed = 0;
    int parked = 0;
    try (Consumer<String, String> consumer =
        consumers.createConsumer(group, null, "replay", overrides)) {
      consumer.subscribe(List.of(dlq));
      boolean assigned = false;
      Instant idleSince = clock.instant();
      while (replayed + parked < max) {
        ConsumerRecords<String, String> records = consumer.poll(POLL);
        Instant now = clock.instant();
        if (!assigned && !consumer.assignment().isEmpty()) {
          // the idle window starts once the group has handed out partitions; the join itself can
          // take longer than the window and must not read as "nothing to replay"
          assigned = true;
          idleSince = now;
        }
        if (records.isEmpty()) {
          if (assigned && Duration.between(idleSince, now).compareTo(IDLE) >= 0) {
            break;
          }
          continue;
        }
        idleSince = now;
        Map<TopicPartition, OffsetAndMetadata> handled = new HashMap<>();
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
          handled.put(
              new TopicPartition(record.topic(), record.partition()),
              new OffsetAndMetadata(record.offset() + 1));
        }
        // only what was republished or parked moves the group forward; a record left in the batch
        // by the budget is polled again by the next replay
        if (!handled.isEmpty()) {
          consumer.commitSync(handled);
        }
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

  /**
   * What is parked per source topic, oldest first, up to {@code max} records per topic. Parked
   * topics are read from the beginning with an assigned consumer that never commits, so inspecting
   * them leaves no trace.
   */
  public Map<String, List<ParkedRecord>> parked(int max) {
    Properties overrides = new Properties();
    overrides.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    overrides.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    Map<String, List<ParkedRecord>> out = new TreeMap<>();
    for (String topic : Topics.ALL) {
      out.put(topic, new ArrayList<>());
    }
    try (Consumer<String, String> consumer =
        consumers.createConsumer(group + "-inspect", null, "parked", overrides)) {
      List<TopicPartition> partitions = new ArrayList<>();
      for (String topic : Topics.ALL) {
        List<PartitionInfo> infos = consumer.partitionsFor(Topics.parked(topic));
        if (infos != null) {
          for (PartitionInfo info : infos) {
            partitions.add(new TopicPartition(info.topic(), info.partition()));
          }
        }
      }
      if (partitions.isEmpty()) {
        return out;
      }
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);
      Map<TopicPartition, Long> ends = consumer.endOffsets(partitions);
      Instant idleSince = clock.instant();
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(POLL);
        for (ConsumerRecord<String, String> record : records) {
          List<ParkedRecord> list = out.get(Topics.sourceOf(record.topic()));
          if (list != null && list.size() < max) {
            list.add(toParked(record));
          }
        }
        boolean caughtUp = true;
        for (TopicPartition partition : partitions) {
          if (consumer.position(partition) < ends.getOrDefault(partition, 0L)) {
            caughtUp = false;
          }
        }
        if (caughtUp) {
          break;
        }
        Instant now = clock.instant();
        if (records.isEmpty()) {
          if (Duration.between(idleSince, now).compareTo(IDLE) >= 0) {
            break;
          }
        } else {
          idleSince = now;
        }
      }
    }
    return out;
  }

  private void park(ConsumerRecord<String, String> record) {
    String target = Topics.sourceOf(record.topic());
    ProducerRecord<String, String> out =
        new ProducerRecord<>(Topics.parked(target), record.key(), record.value());
    // everything travels along: the replay count, the original coordinates and the exception the
    // dead letter publisher stamped, so the parked copy explains itself
    for (Header header : record.headers()) {
      out.headers().add(header);
    }
    out.headers()
        .add(PARKED_AT_HEADER, clock.instant().toString().getBytes(StandardCharsets.UTF_8));
    send(out, "park to " + Topics.parked(target));
    meters.counter("ledgermesh.dlq.parked", "topic", target).increment();
    log.warn(
        "dead letter {}-{}@{} parked on {} after {} replay(s)",
        record.topic(),
        record.partition(),
        record.offset(),
        Topics.parked(target),
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
    send(out, "replay to " + target);
    meters.counter("ledgermesh.dlq.replayed", "topic", target).increment();
  }

  private void send(ProducerRecord<String, String> out, String what) {
    try {
      kafka.send(out).get(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted during " + what, e);
    } catch (ExecutionException | TimeoutException e) {
      throw new IllegalStateException(what + " failed", e);
    }
  }

  private static ParkedRecord toParked(ConsumerRecord<String, String> record) {
    Headers headers = record.headers();
    String parkedAt = text(headers, PARKED_AT_HEADER);
    return new ParkedRecord(
        record.topic(),
        record.partition(),
        record.offset(),
        record.key(),
        PoisonMessagePolicy.replayCount(headers),
        parkedAt == null ? null : Instant.parse(parkedAt),
        text(headers, KafkaHeaders.DLT_ORIGINAL_TOPIC),
        intHeader(headers, KafkaHeaders.DLT_ORIGINAL_PARTITION),
        longHeader(headers, KafkaHeaders.DLT_ORIGINAL_OFFSET),
        text(headers, KafkaHeaders.DLT_EXCEPTION_FQCN),
        text(headers, KafkaHeaders.DLT_EXCEPTION_MESSAGE));
  }

  private static String text(Headers headers, String key) {
    Header header = headers.lastHeader(key);
    return header == null || header.value() == null
        ? null
        : new String(header.value(), StandardCharsets.UTF_8);
  }

  private static Integer intHeader(Headers headers, String key) {
    Header header = headers.lastHeader(key);
    return header == null || header.value() == null || header.value().length != Integer.BYTES
        ? null
        : ByteBuffer.wrap(header.value()).getInt();
  }

  private static Long longHeader(Headers headers, String key) {
    Header header = headers.lastHeader(key);
    return header == null || header.value() == null || header.value().length != Long.BYTES
        ? null
        : ByteBuffer.wrap(header.value()).getLong();
  }
}
