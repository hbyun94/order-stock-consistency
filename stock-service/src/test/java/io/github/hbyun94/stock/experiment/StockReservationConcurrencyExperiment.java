package io.github.hbyun94.stock.experiment;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariDataSource;
import io.github.hbyun94.stock.TestcontainersConfiguration;
import io.github.hbyun94.stock.application.reservation.LockAcquisitionTimeoutException;
import io.github.hbyun94.stock.application.reservation.NoLockStockReservationStrategy;
import io.github.hbyun94.stock.application.reservation.OptimisticLockStockReservationStrategy;
import io.github.hbyun94.stock.application.reservation.PessimisticLockStockReservationStrategy;
import io.github.hbyun94.stock.application.reservation.RedisDistributedLockStockReservationStrategy;
import io.github.hbyun94.stock.application.reservation.ReservationRetryExhaustedException;
import io.github.hbyun94.stock.application.reservation.StockReservationStrategy;
import io.github.hbyun94.stock.domain.InsufficientStockException;
import io.github.hbyun94.stock.domain.Stock;
import io.github.hbyun94.stock.domain.StockRepository;
import io.github.hbyun94.stock.domain.StockReservationRepository;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToLongFunction;
import javax.sql.DataSource;
import jakarta.persistence.EntityManagerFactory;
import org.hibernate.SessionFactory;
import org.hibernate.stat.Statistics;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.util.ClassUtils;

/**
 * 실험 1: 재고 100개에 동시 주문 1,000건 — 락 방식별 초과 판매 건수와 처리 시간 비교.
 * <p>
 * 실행: {@code ./gradlew :stock-service:experiment} (일반 test 작업에서는 제외된다)
 * 결과: 콘솔과 {@code stock-service/build/experiment/experiment-1.md}
 * <p>
 * 측정 방법 (D-016)
 * <ul>
 *   <li>스레드 100개 풀에 요청 1,000건(각 수량 1, 주문 ID 모두 다름)을 넣고 CountDownLatch로 동시에 출발시킨다.</li>
 *   <li>방식마다 예열 1회 후 5회 측정한다. 매회 재고를 100개로 초기화한다.</li>
 *   <li>초과 판매 = 저장된 예약 기록 수 − 100 (0 미만이면 0)</li>
 *   <li>재고 수치 불일치 = 저장된 예약 기록 수 − stock.reserved (lost update로 사라진 증가분)</li>
 *   <li>낙관적 락 충돌 = Hibernate 통계의 optimisticFailureCount (충돌 1회 = 재시도 1회 또는 재시도 소진)</li>
 * </ul>
 */
@Tag("experiment")
// Hibernate 통계를 켜서 낙관적 락 충돌(버전 불일치) 횟수를 센다. 실험에서만 켠다.
@SpringBootTest(properties = "spring.jpa.properties.hibernate.generate_statistics=true")
@Import(TestcontainersConfiguration.class)
class StockReservationConcurrencyExperiment {

    private static final Long PRODUCT_ID = 1L;
    private static final int STOCK = 100;
    private static final int REQUESTS = 1_000;
    private static final int THREADS = 100;
    private static final int WARMUP_RUNS = 1;
    private static final int MEASURED_RUNS = 5;

    // 결과 표의 행 순서
    private static final List<Class<?>> ORDER = List.of(
            NoLockStockReservationStrategy.class,
            OptimisticLockStockReservationStrategy.class,
            PessimisticLockStockReservationStrategy.class,
            RedisDistributedLockStockReservationStrategy.class);

    @Autowired
    List<StockReservationStrategy> strategies;

    @Autowired
    StockRepository stockRepository;

    @Autowired
    StockReservationRepository reservationRepository;

    @Autowired
    DataSource dataSource;

    @Autowired
    EntityManagerFactory entityManagerFactory;

    @Test
    void compareLockStrategies() throws Exception {
        List<StockReservationStrategy> ordered = strategies.stream()
                // @Transactional이 붙은 빈은 CGLIB 프록시이므로 실제 클래스로 비교한다
                .sorted(Comparator.comparingInt(s -> ORDER.indexOf(ClassUtils.getUserClass(s))))
                .toList();

        List<Summary> summaries = new ArrayList<>();
        for (StockReservationStrategy strategy : ordered) {
            for (int i = 0; i < WARMUP_RUNS; i++) {
                runOnce(strategy);
            }
            List<RunResult> runs = new ArrayList<>();
            for (int i = 0; i < MEASURED_RUNS; i++) {
                RunResult run = runOnce(strategy);
                runs.add(run);
                System.out.printf("[실험1] %s #%d: %s%n", strategy.name(), i + 1, run);
            }
            summaries.add(new Summary(strategy, runs));
        }

        String report = report(summaries);
        System.out.println(report);
        Path out = Path.of("build/experiment/experiment-1.md");
        Files.createDirectories(out.getParent());
        Files.writeString(out, report);

        // 락을 쓴 세 방식은 모든 회차에서 초과 판매와 수치 불일치가 0이어야 한다
        summaries.stream()
                .filter(s -> !(s.strategy() instanceof NoLockStockReservationStrategy))
                .forEach(s -> s.runs().forEach(run -> {
                    assertThat(run.oversold()).as(s.strategy().name() + " 초과 판매").isZero();
                    assertThat(run.lostUpdates()).as(s.strategy().name() + " 수치 불일치").isZero();
                    assertThat(run.otherFailures()).as(s.strategy().name() + " 예상 밖 실패").isZero();
                }));
    }

    private RunResult runOnce(StockReservationStrategy strategy) throws InterruptedException {
        reservationRepository.deleteAllInBatch();
        stockRepository.deleteAllInBatch();
        stockRepository.save(Stock.create(PRODUCT_ID, STOCK));
        Statistics statistics = entityManagerFactory.unwrap(SessionFactory.class).getStatistics();
        statistics.clear();

        AtomicInteger success = new AtomicInteger();
        AtomicInteger insufficient = new AtomicInteger();
        AtomicInteger retryExhausted = new AtomicInteger();
        AtomicInteger lockTimeout = new AtomicInteger();
        AtomicInteger other = new AtomicInteger();
        Map<String, Integer> otherTypes = new ConcurrentHashMap<>();

        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(REQUESTS);
        for (long orderId = 1; orderId <= REQUESTS; orderId++) {
            long id = orderId;
            pool.submit(() -> {
                try {
                    start.await();
                    strategy.reserve(id, PRODUCT_ID, 1);
                    success.incrementAndGet();
                } catch (InsufficientStockException e) {
                    insufficient.incrementAndGet();
                } catch (ReservationRetryExhaustedException e) {
                    retryExhausted.incrementAndGet();
                } catch (LockAcquisitionTimeoutException e) {
                    lockTimeout.incrementAndGet();
                } catch (Exception e) {
                    other.incrementAndGet();
                    otherTypes.merge(e.getClass().getSimpleName(), 1, Integer::sum);
                    // 예상 밖 예외는 원인을 남겨 둔다 (근본 원인 메시지 포함)
                    System.out.printf("[실험1] %s 기타 예외: %s / 근본 원인: %s%n",
                            strategy.name(), e, NestedExceptionUtils.getMostSpecificCause(e));
                } finally {
                    done.countDown();
                }
            });
        }

        long startedAt = System.nanoTime();
        start.countDown();
        boolean finished = done.await(5, TimeUnit.MINUTES);
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
        pool.shutdown();
        assertThat(finished).as("5분 안에 끝나지 않음").isTrue();

        int reserved = stockRepository.findByProductId(PRODUCT_ID).orElseThrow().getReserved();
        long reservations = reservationRepository.count(); // 요청 수량이 모두 1이므로 기록 수 = 예약 수량 합
        long conflicts = statistics.getOptimisticFailureCount();
        return new RunResult(success.get(), insufficient.get(), retryExhausted.get(), lockTimeout.get(),
                other.get(), Map.copyOf(otherTypes), reserved, reservations, conflicts, elapsedMillis);
    }

    private String report(List<Summary> summaries) {
        int poolSize = dataSource instanceof HikariDataSource hikari ? hikari.getMaximumPoolSize() : -1;
        StringBuilder sb = new StringBuilder();
        sb.append("## 실험 1 결과 (").append(LocalDate.now()).append(")\n\n");
        sb.append("- 조건: 재고 ").append(STOCK).append("개, 동시 요청 ").append(String.format("%,d", REQUESTS))
                .append("건(각 1개), 스레드 ").append(THREADS).append("개, DB 커넥션 풀 ").append(poolSize)
                .append("개, 예열 ").append(WARMUP_RUNS).append("회 후 ").append(MEASURED_RUNS).append("회 측정\n");
        sb.append("- 환경: Java ").append(Runtime.version()).append(", CPU ")
                .append(Runtime.getRuntime().availableProcessors())
                .append("코어, PostgreSQL 18.7 / Redis 8.10.3 (Testcontainers)\n");
        sb.append("- 표기: 중앙값 (최소~최대)\n\n");
        sb.append("| 방식 | 성공 예약 | 초과 판매 | 재고 수치 불일치 | 재고 부족 실패 | 낙관적 락 충돌(재시도) | 재시도 소진 실패 | 기타 실패 | 처리 시간(ms) |\n");
        sb.append("|---|---|---|---|---|---|---|---|---|\n");
        for (Summary s : summaries) {
            sb.append("| ").append(s.strategy().name())
                    .append(" | ").append(s.stat(RunResult::success))
                    .append(" | ").append(s.stat(RunResult::oversold))
                    .append(" | ").append(s.stat(RunResult::lostUpdates))
                    .append(" | ").append(s.stat(RunResult::insufficient))
                    .append(" | ").append(s.stat(RunResult::optimisticConflicts))
                    .append(" | ").append(s.stat(RunResult::retryExhausted))
                    .append(" | ").append(s.stat(r -> r.other() + r.lockTimeout()))
                    .append(" | ").append(s.stat(RunResult::elapsedMillis))
                    .append(" |\n");
        }
        return sb.toString();
    }

    record RunResult(int success, int insufficient, int retryExhausted, int lockTimeout, int other,
                     Map<String, Integer> otherTypes, int reserved, long reservations, long optimisticConflicts,
                     long elapsedMillis) {

        /** 재고보다 많이 저장된 예약 기록 수 */
        long oversold() {
            return Math.max(0, reservations - STOCK);
        }

        /** 예약 기록은 저장됐는데 stock.reserved에 반영되지 않은 수 (lost update) */
        long lostUpdates() {
            return reservations - reserved;
        }

        long otherFailures() {
            return other + lockTimeout;
        }
    }

    record Summary(StockReservationStrategy strategy, List<RunResult> runs) {

        String stat(ToLongFunction<RunResult> metric) {
            long[] values = runs.stream().mapToLong(metric).sorted().toArray();
            long median = values[values.length / 2];
            long min = values[0];
            long max = values[values.length - 1];
            return min == max ? String.valueOf(median) : median + " (" + min + "~" + max + ")";
        }
    }
}
