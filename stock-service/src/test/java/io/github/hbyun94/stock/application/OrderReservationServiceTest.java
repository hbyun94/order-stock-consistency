package io.github.hbyun94.stock.application;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hbyun94.stock.TestcontainersConfiguration;
import io.github.hbyun94.stock.application.OrderReservationService.Processed;
import io.github.hbyun94.stock.domain.OrderEventResultRepository;
import io.github.hbyun94.stock.domain.RejectionReason;
import io.github.hbyun94.stock.domain.ReservationOutcome;
import io.github.hbyun94.stock.domain.Stock;
import io.github.hbyun94.stock.domain.StockRepository;
import io.github.hbyun94.stock.domain.StockReservationRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

/** 실제 PostgreSQL에서 주문 예약 멱등 처리(D-033)를 검증한다 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class OrderReservationServiceTest {

    private static final Long PRODUCT_ID = 1L;

    @Autowired
    OrderReservationService service;

    @Autowired
    StockService stockService;

    @Autowired
    StockRepository stockRepository;

    @Autowired
    StockReservationRepository reservationRepository;

    @Autowired
    OrderEventResultRepository resultRepository;

    @BeforeEach
    void setUp() {
        resultRepository.deleteAllInBatch();
        reservationRepository.deleteAllInBatch();
        stockRepository.deleteAllInBatch();
        stockRepository.save(Stock.create(PRODUCT_ID, 2));
    }

    @Test
    @DisplayName("같은 주문을 두 번 처리하면 두 번째는 중복으로 판단하고 재고를 다시 건드리지 않는다")
    void duplicateReserved() {
        Processed first = service.process(100L, PRODUCT_ID, 2);
        Processed second = service.process(100L, PRODUCT_ID, 2);

        assertThat(first.duplicate()).isFalse();
        assertThat(first.result().getOutcome()).isEqualTo(ReservationOutcome.RESERVED);
        assertThat(second.duplicate()).isTrue();
        assertThat(second.result().getOutcome()).isEqualTo(ReservationOutcome.RESERVED);
        assertThat(reserved()).isEqualTo(2);
        assertThat(reservationRepository.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("[재고 누수 방지] 재고 부족으로 거절된 주문은 그 사이 재고가 풀려도 중복 처리 때 예약되지 않는다")
    void rejectedStaysRejected() {
        service.process(100L, PRODUCT_ID, 1);                    // 다른 주문이 1개 예약 → 가용 1
        Processed rejected = service.process(200L, PRODUCT_ID, 2); // 2개 요청 → 거절
        stockService.release(100L);                               // 앞 주문 취소 → 가용 2로 복원

        Processed duplicate = service.process(200L, PRODUCT_ID, 2); // 거절됐던 주문이 다시 옴

        assertThat(rejected.result().getOutcome()).isEqualTo(ReservationOutcome.REJECTED);
        assertThat(rejected.result().getReason()).isEqualTo(RejectionReason.INSUFFICIENT_STOCK);
        assertThat(duplicate.duplicate()).isTrue();
        assertThat(duplicate.result().getOutcome()).isEqualTo(ReservationOutcome.REJECTED);
        assertThat(reserved()).isZero();
        assertThat(reservationRepository.findByOrderId(200L)).isEmpty();
    }

    @Test
    @DisplayName("미등록 상품 거절도 기록되어 중복 때 같은 결과를 돌려준다")
    void unknownProductRecorded() {
        Processed first = service.process(300L, 999L, 1);
        Processed second = service.process(300L, 999L, 1);

        assertThat(first.result().getReason()).isEqualTo(RejectionReason.UNKNOWN_PRODUCT);
        assertThat(second.duplicate()).isTrue();
        assertThat(second.result().getReason()).isEqualTo(RejectionReason.UNKNOWN_PRODUCT);
    }

    @Test
    @DisplayName("같은 주문을 동시에 10번 처리해도 결정은 1번, 재고 변경도 1번이다 (락 후 재확인)")
    void concurrentDuplicates() throws Exception {
        List<Processed> results = runConcurrently(10, () -> service.process(100L, PRODUCT_ID, 2));

        assertThat(results).filteredOn(p -> !p.duplicate()).hasSize(1);
        assertThat(results).allSatisfy(p -> assertThat(p.result().getOutcome()).isEqualTo(ReservationOutcome.RESERVED));
        assertThat(reserved()).isEqualTo(2);
        assertThat(reservationRepository.count()).isEqualTo(1);
        assertThat(resultRepository.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("잠글 재고 행이 없는 미등록 상품을 동시에 처리해도 PK 제약이 기록을 1건으로 막는다")
    void concurrentDuplicatesWithoutRowLock() throws Exception {
        List<Processed> results = runConcurrently(10, () -> service.process(300L, 999L, 1));

        assertThat(results).filteredOn(p -> !p.duplicate()).hasSize(1);
        assertThat(results).allSatisfy(p -> assertThat(p.result().getReason()).isEqualTo(RejectionReason.UNKNOWN_PRODUCT));
        assertThat(resultRepository.count()).isEqualTo(1);
    }

    private int reserved() {
        return stockRepository.findByProductId(PRODUCT_ID).orElseThrow().getReserved();
    }

    private static List<Processed> runConcurrently(int threads, java.util.concurrent.Callable<Processed> task)
            throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Processed>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                return task.call();
            }));
        }
        start.countDown();
        List<Processed> results = new ArrayList<>();
        for (Future<Processed> future : futures) {
            results.add(future.get());
        }
        pool.shutdown();
        return results;
    }
}
