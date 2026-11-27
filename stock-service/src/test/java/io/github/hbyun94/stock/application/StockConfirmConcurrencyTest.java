package io.github.hbyun94.stock.application;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hbyun94.stock.TestcontainersConfiguration;
import io.github.hbyun94.stock.domain.ReservationStatus;
import io.github.hbyun94.stock.domain.Stock;
import io.github.hbyun94.stock.domain.StockRepository;
import io.github.hbyun94.stock.domain.StockReservationRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

/**
 * 출고 확정·예약 해제가 실제 흐름에서 동시에 호출돼도 재고 수량이 맞는지 검증한다 (D-048, 실제 PostgreSQL).
 * 확정은 예약 처리(order.created)와 같은 재고 행을 바꾸므로, 락이 없으면 한쪽 변경이 사라진다(lost update).
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class StockConfirmConcurrencyTest {

    private static final int THREADS = 16;

    @Autowired
    StockService stockService;

    @Autowired
    OrderReservationService reservationService;

    @Autowired
    StockRepository stockRepository;

    @Autowired
    StockReservationRepository reservationRepository;

    @Test
    @DisplayName("같은 주문을 동시에 여러 번 확정해도 한 번만 성공하고 실물 재고는 한 번만 줄어든다")
    void concurrentConfirmOfSameOrder() throws Exception {
        long productId = newId();
        long orderId = newId();
        stockRepository.save(Stock.create(productId, 10));
        stockService.reserve(orderId, productId, 3);

        List<Boolean> results = runConcurrently(THREADS, i -> () -> {
            try {
                stockService.confirm(orderId);
                return true;
            } catch (IllegalStateException alreadyConfirmed) {
                return false;
            }
        });

        assertThat(results).filteredOn(ok -> ok).hasSize(1);
        Stock stock = stockRepository.findByProductId(productId).orElseThrow();
        assertThat(stock.getOnHand()).isEqualTo(7);
        assertThat(stock.getReserved()).isZero();
        assertThat(reservationRepository.findByOrderId(orderId).orElseThrow().getStatus())
                .isEqualTo(ReservationStatus.CONFIRMED);
    }

    @Test
    @DisplayName("출고 확정과 새 주문 예약이 같은 상품에 동시에 몰려도 재고 변경이 하나도 사라지지 않는다")
    void confirmAndReserveConcurrentlyOnSameProduct() throws Exception {
        long productId = newId();
        int count = 40;
        stockRepository.save(Stock.create(productId, 100));
        List<Long> reservedOrders = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            long orderId = newId();
            stockService.reserve(orderId, productId, 1);
            reservedOrders.add(orderId);
        }

        // 앞의 40건은 확정하고, 동시에 새 주문 40건을 예약한다
        runConcurrently(count * 2, i -> () -> {
            if (i % 2 == 0) {
                stockService.confirm(reservedOrders.get(i / 2));
            } else {
                reservationService.process(newId(), productId, 1);
            }
            return true;
        });

        Stock stock = stockRepository.findByProductId(productId).orElseThrow();
        assertThat(stock.getOnHand()).isEqualTo(100 - count);  // 확정 40건만큼 출고
        assertThat(stock.getReserved()).isEqualTo(count);      // 확정으로 40 줄고, 새 예약으로 40 늘어남
    }

    private interface TaskFactory {
        Callable<Boolean> create(int index);
    }

    private static List<Boolean> runConcurrently(int tasks, TaskFactory factory) throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Boolean>> futures = new ArrayList<>();
        try (ExecutorService executor = Executors.newFixedThreadPool(THREADS)) {
            for (int i = 0; i < tasks; i++) {
                Callable<Boolean> task = factory.create(i);
                futures.add(executor.submit(() -> {
                    start.await();
                    return task.call();
                }));
            }
            start.countDown();
            List<Boolean> results = new ArrayList<>();
            for (Future<Boolean> future : futures) {
                results.add(future.get(60, TimeUnit.SECONDS));
            }
            return results;
        }
    }

    private static long newId() {
        return ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
    }
}
