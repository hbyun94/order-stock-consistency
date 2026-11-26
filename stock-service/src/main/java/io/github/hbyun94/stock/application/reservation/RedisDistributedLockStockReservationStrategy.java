package io.github.hbyun94.stock.application.reservation;

import io.github.hbyun94.stock.application.StockService;
import io.github.hbyun94.stock.domain.StockReservation;
import java.util.concurrent.TimeUnit;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Redis 분산 락 (Redisson RLock): DB가 아니라 Redis에서 상품 단위로 한 줄 세운다.
 * <p>
 * <b>순서가 핵심이다: 락 획득 → 트랜잭션 시작 → 커밋 → 락 해제.</b>
 * 이 메서드에는 {@code @Transactional}을 붙이지 않는다. 트랜잭션은 락 안에서 호출하는
 * {@link StockService#reserve}(프록시를 거치는 @Transactional 메서드)가 열고 닫는다.
 * 만약 이 메서드에 @Transactional을 붙이면 finally의 unlock이 커밋보다 먼저 실행되고,
 * 그 틈에 다음 요청이 커밋 전의 옛 reserved 값을 읽어 분산 락을 쓰고도 초과 판매가 생긴다.
 * <p>
 * Redisson RLock의 동작:
 * <ul>
 *   <li>대기: 스핀(반복 조회) 대신 Redis Pub/Sub으로 해제 알림을 받아 깨어난다.</li>
 *   <li>만료: leaseTime을 주지 않으면 watchdog이 락을 가진 동안 만료 시간을 자동 연장한다
 *       (기본 30초, 10초마다 연장). 프로세스가 죽으면 연장이 멈춰 락이 만료되고 해제된다.</li>
 *   <li>소유자: 락 값에 클라이언트 ID와 스레드 ID가 들어가서, 다른 스레드가 잘못 해제할 수 없다.</li>
 * </ul>
 */
@Component
public class RedisDistributedLockStockReservationStrategy implements StockReservationStrategy {

    private static final String LOCK_KEY_PREFIX = "lock:stock:";

    private final RedissonClient redissonClient;
    private final StockService stockService;
    private final long waitMillis;

    public RedisDistributedLockStockReservationStrategy(
            RedissonClient redissonClient,
            StockService stockService,
            @Value("${stock.reservation.redis-lock.wait-millis:30000}") long waitMillis) {
        this.redissonClient = redissonClient;
        this.stockService = stockService;
        this.waitMillis = waitMillis;
    }

    @Override
    public String name() {
        return "Redis 분산 락";
    }

    @Override
    public StockReservation reserve(Long orderId, Long productId, int quantity) {
        String lockKey = LOCK_KEY_PREFIX + productId; // 상품 단위 락: 다른 상품 주문끼리는 서로 기다리지 않는다
        RLock lock = redissonClient.getLock(lockKey);

        if (!tryLock(lock)) {
            throw new LockAcquisitionTimeoutException(lockKey, waitMillis);
        }
        try {
            // 트랜잭션은 이 호출 안에서 시작하고 커밋까지 끝난 뒤 반환된다
            return stockService.reserve(orderId, productId, quantity);
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock(); // 커밋이 끝난 뒤에 해제
            }
        }
    }

    private boolean tryLock(RLock lock) {
        try {
            // leaseTime 생략 → watchdog이 만료 시간을 관리한다
            return lock.tryLock(waitMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("분산 락 대기 중 인터럽트", e);
        }
    }
}
