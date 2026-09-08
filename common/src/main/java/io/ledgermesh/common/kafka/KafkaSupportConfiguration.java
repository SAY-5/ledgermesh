package io.ledgermesh.common.kafka;

import io.ledgermesh.common.events.Topics;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.DeserializationException;

/**
 * Topics (and their dead letter shadows) are created on startup by whichever service comes up
 * first. A failing record is retried in place with exponential backoff, which holds the partition
 * while a dependency (database, broker) recovers; once the attempts are used up the record is
 * published to {@code <topic>.dlq} with the original coordinates and the exception in headers, and
 * the partition moves on. Structurally broken payloads skip the retries and go straight to the dead
 * letter topic.
 */
@Configuration
public class KafkaSupportConfiguration {

  public static final String DLQ_PUBLISHED = "ledgermesh.dlq.published";

  @Bean
  public KafkaAdmin.NewTopics ledgermeshTopics() {
    NewTopic[] topics = new NewTopic[Topics.ALL.length * 2];
    for (int i = 0; i < Topics.ALL.length; i++) {
      topics[2 * i] = TopicBuilder.name(Topics.ALL[i]).partitions(3).replicas(1).build();
      topics[2 * i + 1] =
          TopicBuilder.name(Topics.dlq(Topics.ALL[i])).partitions(1).replicas(1).build();
    }
    return new KafkaAdmin.NewTopics(topics);
  }

  @Bean
  public CommonErrorHandler kafkaErrorHandler(
      KafkaTemplate<String, String> kafka,
      MeterRegistry meters,
      @Value("${ledgermesh.dlq.max-attempts:8}") int maxAttempts,
      @Value("${ledgermesh.dlq.initial-backoff-ms:500}") long initialBackoffMs,
      @Value("${ledgermesh.dlq.max-backoff-ms:10000}") long maxBackoffMs) {
    DeadLetterPublishingRecoverer recoverer =
        new DeadLetterPublishingRecoverer(
            kafka,
            (record, cause) -> {
              meters.counter(DLQ_PUBLISHED, "topic", record.topic()).increment();
              return new TopicPartition(Topics.dlq(record.topic()), -1);
            });
    ExponentialBackOffWithMaxRetries backOff =
        new ExponentialBackOffWithMaxRetries(Math.max(0, maxAttempts - 1));
    backOff.setInitialInterval(initialBackoffMs);
    backOff.setMultiplier(2.0);
    backOff.setMaxInterval(maxBackoffMs);
    DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
    handler.addNotRetryableExceptions(
        DeserializationException.class, IllegalArgumentException.class);
    return handler;
  }
}
