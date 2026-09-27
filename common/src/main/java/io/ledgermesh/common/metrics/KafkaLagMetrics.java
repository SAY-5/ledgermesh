package io.ledgermesh.common.metrics;

import io.ledgermesh.common.events.Topics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Consumer lag for this service's listener groups, dead letter depth and parked depth per topic,
 * computed from broker offsets on a fixed delay. Lag is end offset minus committed offset summed
 * over partitions; dead letter depth is the same difference for the replay group on {@code
 * <topic>.dlq}, so a replayed or parked record leaves the depth; parked depth is every record
 * retained on {@code <topic>.parked}, which nobody consumes. All three are gauges, so a scrape
 * between refreshes sees the last computed value.
 */
public class KafkaLagMetrics implements DisposableBean {

  public static final String LAG = "ledgermesh.consumer.lag";
  public static final String DLQ_DEPTH = "ledgermesh.dlq.depth";
  public static final String PARKED_DEPTH = "ledgermesh.dlq.parked.depth";

  private static final Logger log = LoggerFactory.getLogger(KafkaLagMetrics.class);
  private static final long TIMEOUT_SECONDS = 5;

  private final AdminClient admin;
  private final KafkaListenerEndpointRegistry listeners;
  private final MeterRegistry meters;
  private final String replayGroup;
  private final Map<String, AtomicLong> lag = new ConcurrentHashMap<>();
  private final Map<String, AtomicLong> depth = new ConcurrentHashMap<>();
  private final Map<String, AtomicLong> parked = new ConcurrentHashMap<>();

  public KafkaLagMetrics(
      KafkaAdmin kafkaAdmin,
      KafkaListenerEndpointRegistry listeners,
      MeterRegistry meters,
      String replayGroup) {
    this.admin = AdminClient.create(kafkaAdmin.getConfigurationProperties());
    this.listeners = listeners;
    this.meters = meters;
    this.replayGroup = replayGroup;
  }

  @Scheduled(fixedDelayString = "${ledgermesh.kafka.lag-poll-ms:5000}")
  public void tick() {
    try {
      refresh();
    } catch (RuntimeException e) {
      log.debug("lag refresh skipped: {}", e.toString());
    }
  }

  /** Recomputes every gauge from broker offsets. Throws when the broker cannot be reached. */
  public void refresh() {
    Set<String> topics = new LinkedHashSet<>(List.of(Topics.ALL));
    Map<String, Set<String>> groups = new HashMap<>();
    for (MessageListenerContainer container : listeners.getListenerContainers()) {
      String[] subscribed = container.getContainerProperties().getTopics();
      if (container.getGroupId() == null || subscribed == null) {
        continue;
      }
      Set<String> mine = groups.computeIfAbsent(container.getGroupId(), g -> new LinkedHashSet<>());
      for (String topic : subscribed) {
        if (!Topics.isDlq(topic)) {
          mine.add(topic);
          topics.add(topic);
        }
      }
    }
    try {
      Map<String, TopicDescription> described = describe(topics);
      for (Map.Entry<String, Set<String>> group : groups.entrySet()) {
        Map<TopicPartition, Long> committed = committed(group.getKey());
        for (String topic : group.getValue()) {
          gauge(
                  lag,
                  group.getKey() + "|" + topic,
                  LAG,
                  Tags.of("group", group.getKey(), "topic", topic))
              .set(behind(described.get(topic), committed));
        }
      }
      Map<String, TopicDescription> dlqs =
          describe(topics.stream().map(Topics::dlq).collect(java.util.stream.Collectors.toSet()));
      Map<TopicPartition, Long> replayed = committed(replayGroup);
      for (String topic : topics) {
        gauge(depth, topic, DLQ_DEPTH, Tags.of("topic", topic))
            .set(behind(dlqs.get(Topics.dlq(topic)), replayed));
      }
      Map<String, TopicDescription> parkedTopics =
          describe(
              topics.stream().map(Topics::parked).collect(java.util.stream.Collectors.toSet()));
      for (String topic : topics) {
        gauge(parked, topic, PARKED_DEPTH, Tags.of("topic", topic))
            .set(behind(parkedTopics.get(Topics.parked(topic)), Map.of()));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while reading offsets", e);
    } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
      throw new IllegalStateException("could not read offsets", e);
    }
  }

  /** Dead letter depth per source topic from the last refresh, sorted by topic. */
  public Map<String, Long> dlqDepth() {
    Map<String, Long> out = new TreeMap<>();
    depth.forEach((topic, value) -> out.put(topic, value.get()));
    return out;
  }

  /** Records parked per source topic from the last refresh, sorted by topic. */
  public Map<String, Long> parkedDepth() {
    Map<String, Long> out = new TreeMap<>();
    parked.forEach((topic, value) -> out.put(topic, value.get()));
    return out;
  }

  /** Lag per {@code group|topic} from the last refresh. */
  public Map<String, Long> consumerLag() {
    Map<String, Long> out = new TreeMap<>();
    lag.forEach((key, value) -> out.put(key, value.get()));
    return out;
  }

  private long behind(TopicDescription description, Map<TopicPartition, Long> committed)
      throws InterruptedException,
          java.util.concurrent.ExecutionException,
          java.util.concurrent.TimeoutException {
    if (description == null) {
      return 0;
    }
    Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
    Map<TopicPartition, OffsetSpec> earliest = new HashMap<>();
    description
        .partitions()
        .forEach(
            p -> {
              TopicPartition tp = new TopicPartition(description.name(), p.partition());
              latest.put(tp, OffsetSpec.latest());
              earliest.put(tp, OffsetSpec.earliest());
            });
    Map<TopicPartition, ListOffsetsResultInfo> ends =
        admin.listOffsets(latest).all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    Map<TopicPartition, ListOffsetsResultInfo> starts =
        admin.listOffsets(earliest).all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    long total = 0;
    for (Map.Entry<TopicPartition, ListOffsetsResultInfo> end : ends.entrySet()) {
      long from = committed.getOrDefault(end.getKey(), starts.get(end.getKey()).offset());
      total += Math.max(0, end.getValue().offset() - from);
    }
    return total;
  }

  private Map<String, TopicDescription> describe(Set<String> topics)
      throws InterruptedException,
          java.util.concurrent.ExecutionException,
          java.util.concurrent.TimeoutException {
    Map<String, TopicDescription> out = new HashMap<>();
    admin
        .describeTopics(topics)
        .topicNameValues()
        .forEach(
            (name, future) -> {
              try {
                out.put(name, future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
              } catch (java.util.concurrent.ExecutionException
                  | java.util.concurrent.TimeoutException e) {
                log.debug("topic {} not described: {}", name, e.toString());
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    return out;
  }

  private Map<TopicPartition, Long> committed(String group)
      throws InterruptedException,
          java.util.concurrent.ExecutionException,
          java.util.concurrent.TimeoutException {
    Map<TopicPartition, Long> out = new HashMap<>();
    Map<TopicPartition, OffsetAndMetadata> offsets =
        admin
            .listConsumerGroupOffsets(group)
            .partitionsToOffsetAndMetadata()
            .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    offsets.forEach((tp, om) -> out.put(tp, om.offset()));
    return out;
  }

  private AtomicLong gauge(Map<String, AtomicLong> store, String key, String name, Tags tags) {
    return store.computeIfAbsent(key, k -> meters.gauge(name, tags, new AtomicLong()));
  }

  @Override
  public void destroy() {
    admin.close();
  }
}
