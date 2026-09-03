package io.ledgermesh.common.kafka;

import io.ledgermesh.common.events.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Topics are created on startup by whichever service comes up first. The error handler retries a
 * failing record forever with a short pause: the record stays uncommitted, the partition is held,
 * and the work resumes when the dependency (database, broker) is back. Only structurally broken
 * payloads are skipped.
 */
@Configuration
public class KafkaSupportConfiguration {

  @Bean
  public KafkaAdmin.NewTopics ledgermeshTopics() {
    NewTopic[] topics = new NewTopic[Topics.ALL.length];
    for (int i = 0; i < Topics.ALL.length; i++) {
      topics[i] = TopicBuilder.name(Topics.ALL[i]).partitions(3).replicas(1).build();
    }
    return new KafkaAdmin.NewTopics(topics);
  }

  @Bean
  public CommonErrorHandler kafkaErrorHandler() {
    DefaultErrorHandler handler =
        new DefaultErrorHandler(new FixedBackOff(500L, FixedBackOff.UNLIMITED_ATTEMPTS));
    handler.addNotRetryableExceptions(DeserializationException.class, IllegalArgumentException.class);
    return handler;
  }
}
