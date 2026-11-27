package io.github.hbyun94.stock.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.hbyun94.stock.TestcontainersConfiguration;
import io.github.hbyun94.stock.application.OrderReservationService.Processed;
import io.github.hbyun94.stock.domain.ReservationOutcome;
import io.github.hbyun94.stock.domain.Stock;
import io.github.hbyun94.stock.domain.StockRepository;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 예약 흐름의 비관적 락 대기 상한(D-047)을 실제 PostgreSQL에서 검증한다.
 * 다른 트랜잭션이 재고 행의 락을 쥐고 놓지 않을 때, 예약은 무한히 기다리지 않고 약 3초 뒤 실패해야 한다.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class ReservationLockTimeoutTest {

    @Autowired
    OrderReservationService reservationService;

    @Autowired
    StockRepository stockRepository;

    @Autowired
    TransactionTemplate transactionTemplate;

    @Test
    @DisplayName("다른 트랜잭션이 락을 쥐고 있으면 약 3초 뒤 락 실패로 끝나고, 락이 풀리면 같은 주문을 정상 처리한다")
    void lockWaitIsBounded() throws Exception {
        long productId = ThreadLocalRandom.current().nextLong(1, 1_000_000);
        long orderId = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
        stockRepository.save(Stock.create(productId, 5));

        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        // 락을 쥐고 놓지 않는 트랜잭션 (느려진 출고 확정 같은 상황)
        CompletableFuture<Void> holder = CompletableFuture.runAsync(() -> transactionTemplate.executeWithoutResult(status -> {
            stockRepository.findByProductIdForUpdate(productId).orElseThrow();
            locked.countDown();
            try {
                release.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
        try {
            assertThat(locked.await(10, TimeUnit.SECONDS)).isTrue();

            long start = System.nanoTime();
            assertThatThrownBy(() -> reservationService.process(orderId, productId, 1))
                    .isInstanceOf(PessimisticLockingFailureException.class);
            Duration waited = Duration.ofNanos(System.nanoTime() - start);

            // 3초를 기다린 뒤 실패한다: 바로 실패하지도(NOWAIT 아님), 무한히 기다리지도 않는다
            assertThat(waited).isBetween(Duration.ofMillis(2_500), Duration.ofSeconds(10));
        } finally {
            release.countDown();
            holder.get(30, TimeUnit.SECONDS);
        }

        // 실패한 시도는 통째로 롤백됐으므로, 다시 처리하면(재시도) 정상 예약된다
        Processed processed = reservationService.process(orderId, productId, 1);
        assertThat(processed.duplicate()).isFalse();
        assertThat(processed.result().getOutcome()).isEqualTo(ReservationOutcome.RESERVED);
        assertThat(stockRepository.findByProductId(productId).orElseThrow().getReserved()).isEqualTo(1);
    }
}
