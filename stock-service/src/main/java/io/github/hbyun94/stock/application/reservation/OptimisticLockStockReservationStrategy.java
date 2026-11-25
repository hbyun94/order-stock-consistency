package io.github.hbyun94.stock.application.reservation;

import io.github.hbyun94.stock.application.StockNotFoundException;
import io.github.hbyun94.stock.domain.StockReservation;
import io.github.hbyun94.stock.domain.StockReservationRepository;
import io.github.hbyun94.stock.domain.VersionedStock;
import io.github.hbyun94.stock.domain.VersionedStockRepository;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 낙관적 락: 락을 잡지 않고 진행하다가, 커밋 시점에 version이 바뀌었으면 실패시키고 재시도한다.
 * <p>
 * <b>재시도는 반드시 트랜잭션 바깥에서 한다.</b>
 * 한 번 실패한 트랜잭션은 롤백 전용 상태가 되고, 영속성 컨텍스트에는 옛 version을 가진 엔티티가 남아 있다.
 * 같은 트랜잭션 안에서 다시 시도하면 옛 값으로 또 충돌한다.
 * 그래서 시도할 때마다 {@link TransactionTemplate}으로 새 트랜잭션(= 새 영속성 컨텍스트, 새 조회)을 연다.
 * <p>
 * 재시도 사이에는 무작위 대기(jitter)를 둔다. 모두 같은 간격으로 재시도하면 다시 동시에 부딪히기 때문이다.
 */
@Component
public class OptimisticLockStockReservationStrategy implements StockReservationStrategy {

    private final VersionedStockRepository stockRepository;
    private final StockReservationRepository reservationRepository;
    private final TransactionTemplate transactionTemplate;
    private final int maxAttempts;
    private final long minBackoffMillis;
    private final long maxBackoffMillis;

    public OptimisticLockStockReservationStrategy(
            VersionedStockRepository stockRepository,
            StockReservationRepository reservationRepository,
            TransactionTemplate transactionTemplate,
            @Value("${stock.reservation.optimistic.max-attempts:10}") int maxAttempts,
            @Value("${stock.reservation.optimistic.min-backoff-millis:10}") long minBackoffMillis,
            @Value("${stock.reservation.optimistic.max-backoff-millis:50}") long maxBackoffMillis) {
        this.stockRepository = stockRepository;
        this.reservationRepository = reservationRepository;
        this.transactionTemplate = transactionTemplate;
        this.maxAttempts = maxAttempts;
        this.minBackoffMillis = minBackoffMillis;
        this.maxBackoffMillis = maxBackoffMillis;
    }

    @Override
    public String name() {
        return "낙관적 락";
    }

    @Override
    public StockReservation reserve(Long orderId, Long productId, int quantity) {
        OptimisticLockingFailureException lastFailure = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                // 시도마다 새 트랜잭션. 충돌은 커밋(flush) 시점의 UPDATE ... WHERE version = ? 에서 감지된다
                return transactionTemplate.execute(status -> reserveOnce(orderId, productId, quantity));
            } catch (OptimisticLockingFailureException e) {
                lastFailure = e;
                if (attempt < maxAttempts) {
                    backoff();
                }
            }
            // InsufficientStockException 등 다른 예외는 재시도해도 결과가 같으므로 그대로 던진다
        }
        throw new ReservationRetryExhaustedException(orderId, maxAttempts, lastFailure);
    }

    private StockReservation reserveOnce(Long orderId, Long productId, int quantity) {
        VersionedStock stock = stockRepository.findByProductId(productId)
                .orElseThrow(() -> new StockNotFoundException(productId));
        stock.reserve(quantity);
        return reservationRepository.save(StockReservation.reserve(orderId, productId, quantity));
    }

    private void backoff() {
        long millis = ThreadLocalRandom.current().nextLong(minBackoffMillis, maxBackoffMillis + 1);
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("재시도 대기 중 인터럽트", e);
        }
    }
}
