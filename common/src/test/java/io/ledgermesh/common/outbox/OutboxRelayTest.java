package io.ledgermesh.common.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

@DataJpaTest
class OutboxRelayTest {

  private static final Clock CLOCK =
      Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);

  @Autowired private OutboxEventRepository repository;

  @SuppressWarnings("unchecked")
  private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);

  private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

  private OutboxRelay relay;

  @BeforeEach
  void setUp() {
    relay = new OutboxRelay(repository, kafka, CLOCK, meters, 1000);
    repository.save(row("e1", "order-1"));
    repository.save(row("e2", "order-1"));
    repository.save(row("e3", "order-2"));
  }

  @Test
  void publishesPendingRowsInInsertionOrderAndMarksThem() {
    when(kafka.send(any(ProducerRecord.class))).thenReturn(acked());

    assertThat(relay.relayPending()).isEqualTo(3);

    ArgumentCaptor<ProducerRecord<String, String>> captor =
        ArgumentCaptor.forClass(ProducerRecord.class);
    verify(kafka, times(3)).send(captor.capture());
    assertThat(captor.getAllValues())
        .extracting(ProducerRecord::key)
        .containsExactly("order-1", "order-1", "order-2");
    assertThat(repository.countByPublishedAtIsNull()).isZero();
    assertThat(repository.findAll())
        .allSatisfy(r -> assertThat(r.getPublishedAt()).isEqualTo(CLOCK.instant()));
  }

  @Test
  void brokerFailureLeavesRowPendingAndStopsTheBatchToPreserveOrder() {
    when(kafka.send(any(ProducerRecord.class)))
        .thenReturn(acked())
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

    assertThat(relay.relayPending()).isEqualTo(1);
    assertThat(repository.countByPublishedAtIsNull()).isEqualTo(2);

    when(kafka.send(any(ProducerRecord.class))).thenReturn(acked());
    assertThat(relay.relayPending()).isEqualTo(2);
    assertThat(repository.countByPublishedAtIsNull()).isZero();
  }

  @Test
  void countsARowWhoseLastAttemptNeverConfirmedAsAResend() {
    when(kafka.send(any(ProducerRecord.class))).thenReturn(acked());
    OutboxEvent crashed = repository.findAll().get(0);
    crashed.markAttempted(CLOCK.instant());
    repository.saveAndFlush(crashed);

    assertThat(relay.relayPending()).isEqualTo(3);

    assertThat(meters.counter("ledgermesh.outbox.resends").count()).isEqualTo(1);
    assertThat(repository.countByPublishedAtIsNull()).isZero();
  }

  @Test
  void secondRunIsNoOpOncePublished() {
    when(kafka.send(any(ProducerRecord.class))).thenReturn(acked());
    relay.relayPending();

    assertThat(relay.relayPending()).isZero();
    verify(kafka, times(3)).send(any(ProducerRecord.class));
  }

  @Test
  void carriesCorrelationAndEventIdHeaders() {
    when(kafka.send(any(ProducerRecord.class))).thenReturn(acked());
    relay.relayPending();

    ArgumentCaptor<ProducerRecord<String, String>> captor =
        ArgumentCaptor.forClass(ProducerRecord.class);
    verify(kafka, times(3)).send(captor.capture());
    ProducerRecord<String, String> first = captor.getAllValues().get(0);
    assertThat(new String(first.headers().lastHeader("x-correlation-id").value()))
        .isEqualTo("corr");
    assertThat(new String(first.headers().lastHeader("event-id").value())).isEqualTo("e1");
    assertThat(List.of(first.topic())).containsExactly("order.created");
  }

  private static OutboxEvent row(String eventId, String orderId) {
    return new OutboxEvent(
        eventId, "order.created", orderId, "OrderCreated", "corr", "{}", CLOCK.instant());
  }

  private static CompletableFuture<SendResult<String, String>> acked() {
    return CompletableFuture.completedFuture(mock(SendResult.class));
  }
}
