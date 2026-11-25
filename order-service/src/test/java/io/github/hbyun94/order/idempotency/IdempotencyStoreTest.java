package io.github.hbyun94.order.idempotency;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hbyun94.order.TestcontainersConfiguration;
import io.github.hbyun94.order.idempotency.IdempotencyState.Acquired;
import io.github.hbyun94.order.idempotency.IdempotencyState.Completed;
import io.github.hbyun94.order.idempotency.IdempotencyState.InProgress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;

/** 실제 Redis에서 멱등 키 선점/완료/해제 동작을 검증한다 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class IdempotencyStoreTest {

    private static final String FP = "a".repeat(64);
    private static final String OTHER_FP = "b".repeat(64);

    @Autowired
    IdempotencyStore store;

    @Autowired
    StringRedisTemplate redis;

    @Test
    @DisplayName("처음 선점하면 Acquired, 같은 키로 다시 시도하면 InProgress다")
    void acquireThenInProgress() {
        String key = newKey();

        assertThat(store.tryAcquire(key, FP)).isInstanceOf(Acquired.class);
        assertThat(store.tryAcquire(key, FP)).isEqualTo(new InProgress(FP));
    }

    @Test
    @DisplayName("처리 중 키에는 짧은 TTL(30초 이하)이 걸려 서버가 죽어도 키가 영원히 남지 않는다")
    void processingKeyHasShortTtl() {
        String key = newKey();
        store.tryAcquire(key, FP);

        Long ttlSeconds = redis.getExpire(IdempotencyStore.KEY_PREFIX + key);
        assertThat(ttlSeconds).isBetween(1L, 30L);
    }

    @Test
    @DisplayName("완료하면 Completed와 주문 ID가 저장되고 TTL이 24시간으로 늘어난다")
    void complete() {
        String key = newKey();
        store.tryAcquire(key, FP);

        store.complete(key, FP, 42L);

        assertThat(store.tryAcquire(key, FP)).isEqualTo(new Completed(FP, 42L));
        assertThat(redis.getExpire(IdempotencyStore.KEY_PREFIX + key)).isGreaterThan(23 * 3600L);
    }

    @Test
    @DisplayName("해제하면 같은 키로 다시 선점할 수 있다 (처리 실패 후 재시도 허용)")
    void release() {
        String key = newKey();
        store.tryAcquire(key, FP);

        store.release(key, FP);

        assertThat(store.tryAcquire(key, FP)).isInstanceOf(Acquired.class);
    }

    @Test
    @DisplayName("이미 완료된 키는 해제 요청이 와도 지워지지 않는다 (내가 선점한 PROCESSING 값일 때만 지운다)")
    void releaseDoesNotRemoveCompleted() {
        String key = newKey();
        store.tryAcquire(key, FP);
        store.complete(key, FP, 42L);

        store.release(key, FP);

        assertThat(store.tryAcquire(key, FP)).isEqualTo(new Completed(FP, 42L));
    }

    @Test
    @DisplayName("다른 지문의 해제 요청은 남의 선점을 지우지 못한다")
    void releaseWithOtherFingerprint() {
        String key = newKey();
        store.tryAcquire(key, FP);

        store.release(key, OTHER_FP);

        assertThat(store.tryAcquire(key, FP)).isEqualTo(new InProgress(FP));
    }

    @Test
    @DisplayName("같은 키로 동시에 20번 선점해도 정확히 1번만 Acquired다")
    void concurrentAcquire() throws Exception {
        String key = newKey();
        int threads = 20;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<IdempotencyState>> results = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            results.add(pool.submit(() -> {
                start.await();
                return store.tryAcquire(key, FP);
            }));
        }
        start.countDown();

        int acquired = 0;
        for (Future<IdempotencyState> result : results) {
            if (result.get() instanceof Acquired) {
                acquired++;
            }
        }
        pool.shutdown();
        assertThat(acquired).isEqualTo(1);
    }

    private static String newKey() {
        return UUID.randomUUID().toString();
    }
}
