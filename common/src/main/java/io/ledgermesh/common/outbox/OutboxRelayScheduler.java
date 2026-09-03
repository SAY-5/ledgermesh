package io.ledgermesh.common.outbox;

import org.springframework.scheduling.annotation.Scheduled;

/** Drives the relay on a fixed delay so runs never overlap. */
public class OutboxRelayScheduler {

  private final OutboxRelay relay;

  public OutboxRelayScheduler(OutboxRelay relay) {
    this.relay = relay;
  }

  @Scheduled(fixedDelayString = "${ledgermesh.outbox.poll-ms:200}")
  public void tick() {
    relay.relayPending();
  }
}
