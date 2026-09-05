package io.ledgermesh.inventory.stock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;

import io.ledgermesh.common.events.OrderLine;
import io.ledgermesh.inventory.cache.StockCache;
import io.ledgermesh.inventory.stock.ReservationService.Outcome;
import io.ledgermesh.inventory.stock.ReservationService.Rejected;
import io.ledgermesh.inventory.stock.ReservationService.Reserved;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest
class ReservationServiceTest {

  @Autowired private ReservationService reservations;
  @Autowired private StockRepository stock;
  @MockitoBean private StockCache cache;

  @BeforeEach
  void seed() {
    doNothing().when(cache).writeThrough(anyString(), anyInt());
    stock.deleteAll();
    stock.save(new StockItem("A", 10));
    stock.save(new StockItem("B", 1));
  }

  @Test
  void reservesEveryLineAndReportsRemainingStock() {
    Outcome outcome = reservations.reserve(List.of(new OrderLine("A", 3), new OrderLine("B", 1)));

    assertThat(outcome).isInstanceOf(Reserved.class);
    assertThat(((Reserved) outcome).remaining()).containsEntry("A", 7).containsEntry("B", 0);
    assertThat(stock.findById("A").orElseThrow().getReserved()).isEqualTo(3);
  }

  @Test
  void multiLineOrderIsAllOrNothing() {
    Outcome outcome = reservations.reserve(List.of(new OrderLine("A", 3), new OrderLine("B", 2)));

    assertThat(outcome).isEqualTo(new Rejected("OUT_OF_STOCK"));
    assertThat(stock.findById("A").orElseThrow().getAvailable()).isEqualTo(10);
  }

  @Test
  void unknownSkuIsRejected() {
    assertThat(reservations.reserve(List.of(new OrderLine("ZZZ", 1)))).isEqualTo(new Rejected("UNKNOWN_SKU"));
  }

  @Test
  void releaseReturnsReservedUnits() {
    reservations.reserve(List.of(new OrderLine("A", 4)));

    reservations.release(List.of(new OrderLine("A", 4)));

    StockItem a = stock.findById("A").orElseThrow();
    assertThat(a.getAvailable()).isEqualTo(10);
    assertThat(a.getReserved()).isZero();
  }

  @Test
  void releaseNeverExceedsWhatWasReserved() {
    reservations.reserve(List.of(new OrderLine("A", 2)));

    reservations.release(List.of(new OrderLine("A", 5)));

    assertThat(stock.findById("A").orElseThrow().getAvailable()).isEqualTo(10);
  }

  @Test
  void concurrentReservationsNeverOversell() throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(8);
    try {
      List<Future<Outcome>> results =
          IntStream.range(0, 25)
              .mapToObj(i -> pool.submit(() -> reservations.reserve(List.of(new OrderLine("A", 1)))))
              .toList();
      long reserved = 0;
      for (Future<Outcome> f : results) {
        if (f.get() instanceof Reserved) {
          reserved++;
        }
      }
      assertThat(reserved).isEqualTo(10);
      assertThat(stock.findById("A").orElseThrow().getAvailable()).isZero();
    } finally {
      pool.shutdownNow();
    }
  }
}
