package io.github.hbyun94.order.idempotency;

import io.github.hbyun94.order.idempotency.IdempotencyState.Acquired;
import io.github.hbyun94.order.idempotency.IdempotencyState.Completed;
import io.github.hbyun94.order.idempotency.IdempotencyState.InProgress;
import java.time.Duration;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

/**
 * Redis에 주문 요청 멱등 키를 저장한다 (D-019).
 * <p>
 * 키: {@code idempotency:order:{Idempotency-Key}}
 * 값:
 * <ul>
 *   <li>{@code PROCESSING:{지문}} — 처리 중. 짧은 TTL(30초): 처리 중에 서버가 죽어도 키가 영원히 남지 않게 한다.</li>
 *   <li>{@code COMPLETED:{지문}:{주문ID}} — 완료. 긴 TTL(24시간): 클라이언트 재시도 창 동안 같은 응답을 돌려준다.</li>
 * </ul>
 * Redis와 DB는 하나의 트랜잭션으로 묶을 수 없으므로 이 저장소는 "빠른 1차 차단"이다.
 * 최종 방어선은 orders.idempotency_key UNIQUE 제약이다.
 */
@Component
public class IdempotencyStore {

    static final String KEY_PREFIX = "idempotency:order:";
    private static final String PROCESSING = "PROCESSING";
    private static final String COMPLETED = "COMPLETED";

    /** 값이 내가 넣은 PROCESSING 값과 같을 때만 지운다 (GET과 DEL 사이에 다른 요청이 끼어들지 못하게 원자적으로) */
    private static final RedisScript<Long> RELEASE_IF_MATCHES = RedisScript.of("""
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            end
            return 0
            """, Long.class);

    private final StringRedisTemplate redis;
    private final Duration processingTtl;
    private final Duration completedTtl;

    public IdempotencyStore(
            StringRedisTemplate redis,
            @Value("${order.idempotency.processing-ttl:30s}") Duration processingTtl,
            @Value("${order.idempotency.completed-ttl:24h}") Duration completedTtl) {
        this.redis = redis;
        this.processingTtl = processingTtl;
        this.completedTtl = completedTtl;
    }

    /**
     * 키 선점을 시도한다. {@code SET key value NX EX ttl}은 "없을 때만 쓰기 + 만료 설정"을 한 명령으로 처리하므로,
     * 동시에 여러 요청이 와도 정확히 하나만 {@link Acquired}를 받는다.
     */
    public IdempotencyState tryAcquire(String idempotencyKey, String fingerprint) {
        String key = KEY_PREFIX + idempotencyKey;
        // 선점 실패 후 GET 사이에 키가 만료될 수 있어 몇 번 다시 시도한다
        for (int attempt = 0; attempt < 3; attempt++) {
            Boolean acquired = redis.opsForValue().setIfAbsent(key, processing(fingerprint), processingTtl);
            if (Boolean.TRUE.equals(acquired)) {
                return new Acquired();
            }
            String value = redis.opsForValue().get(key);
            if (value != null) {
                return parse(value);
            }
        }
        throw new IllegalStateException("멱등 키 상태를 확인하지 못했습니다: " + idempotencyKey);
    }

    /** 주문 저장(커밋)이 끝난 뒤 결과를 기록한다. 같은 키의 재요청은 이 주문 ID로 같은 응답을 받는다. */
    public void complete(String idempotencyKey, String fingerprint, long orderId) {
        redis.opsForValue().set(KEY_PREFIX + idempotencyKey, COMPLETED + ":" + fingerprint + ":" + orderId, completedTtl);
    }

    /** 처리 실패 시 선점을 풀어 클라이언트가 같은 키로 재시도할 수 있게 한다. 내가 선점한 값일 때만 지운다. */
    public void release(String idempotencyKey, String fingerprint) {
        redis.execute(RELEASE_IF_MATCHES, List.of(KEY_PREFIX + idempotencyKey), processing(fingerprint));
    }

    private static String processing(String fingerprint) {
        return PROCESSING + ":" + fingerprint;
    }

    private static IdempotencyState parse(String value) {
        String[] parts = value.split(":");
        return switch (parts[0]) {
            case PROCESSING -> new InProgress(parts[1]);
            case COMPLETED -> new Completed(parts[1], Long.parseLong(parts[2]));
            default -> throw new IllegalStateException("알 수 없는 멱등 키 값: " + value);
        };
    }
}
