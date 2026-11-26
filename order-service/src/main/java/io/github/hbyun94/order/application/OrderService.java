package io.github.hbyun94.order.application;

import io.github.hbyun94.order.domain.Order;
import io.github.hbyun94.order.domain.OrderRepository;
import io.github.hbyun94.order.idempotency.IdempotencyState;
import io.github.hbyun94.order.idempotency.IdempotencyState.Acquired;
import io.github.hbyun94.order.idempotency.IdempotencyState.Completed;
import io.github.hbyun94.order.idempotency.IdempotencyState.InProgress;
import io.github.hbyun94.order.idempotency.IdempotencyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 주문 생성. 멱등 처리를 서비스 계층에서 명시적으로 한다 (D-021).
 * <p>
 * 2단 방어 (D-019):
 * <ol>
 *   <li>Redis 멱등 키: 처리 중이면 409, 완료됐으면 저장된 주문을 그대로 다시 돌려준다. 대부분의 중복은 여기서 DB까지 가지 않는다.</li>
 *   <li>DB UNIQUE(idempotency_key): Redis 키가 만료·유실됐거나 Redis 장애로 1단계를 건너뛰어도 중복 주문은 저장되지 않는다.</li>
 * </ol>
 * 순서: Redis 선점 → DB 트랜잭션 커밋 → Redis에 완료 기록. 커밋 전에 "완료"를 쓰면,
 * 커밋이 실패했을 때 존재하지 않는 주문을 재요청에 돌려주게 되기 때문이다.
 */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final IdempotencyStore idempotencyStore;
    private final TransactionTemplate transactionTemplate;
    private final ApplicationEventPublisher eventPublisher;

    public OrderService(
            OrderRepository orderRepository,
            IdempotencyStore idempotencyStore,
            TransactionTemplate transactionTemplate,
            ApplicationEventPublisher eventPublisher) {
        this.orderRepository = orderRepository;
        this.idempotencyStore = idempotencyStore;
        this.transactionTemplate = transactionTemplate;
        this.eventPublisher = eventPublisher;
    }

    public OrderResult placeOrder(String idempotencyKey, PlaceOrderCommand command) {
        String fingerprint = RequestFingerprint.of(command);

        IdempotencyState state;
        try {
            state = idempotencyStore.tryAcquire(idempotencyKey, fingerprint);
        } catch (DataAccessException e) {
            // Redis 장애 시 주문을 막지 않는다(fail-open). 중복은 DB UNIQUE 제약이 막는다.
            log.warn("Redis 멱등 키 확인 실패, DB 제약만으로 처리합니다: key={}", idempotencyKey, e);
            return create(idempotencyKey, fingerprint, command, false);
        }

        return switch (state) {
            case Completed completed -> {
                requireSameRequest(idempotencyKey, completed.fingerprint(), fingerprint);
                yield OrderResult.replayed(orderRepository.findById(completed.orderId())
                        .orElseThrow(() -> new IllegalStateException(
                                "Redis에 완료로 기록된 주문이 DB에 없습니다: orderId=" + completed.orderId())));
            }
            case InProgress inProgress -> {
                requireSameRequest(idempotencyKey, inProgress.fingerprint(), fingerprint);
                throw new IdempotentRequestInProgressException(idempotencyKey);
            }
            case Acquired acquired -> create(idempotencyKey, fingerprint, command, true);
        };
    }

    @Transactional(readOnly = true)
    public Order getOrder(Long orderId) {
        return orderRepository.findById(orderId).orElseThrow(() -> new OrderNotFoundException(orderId));
    }

    /**
     * 재고 예약 성공 이벤트 처리: PENDING → RESERVED.
     * 중복이면 무시하고, 모순(이미 취소된 주문)이면 경고를 남기고 넘어간다. 어느 쪽도 예외를 던지지 않아
     * 같은 메시지가 무한히 재시도되지 않는다 (D-034).
     */
    @Transactional
    public void markReserved(Long orderId) {
        Order order = findOrder(orderId);
        try {
            if (!order.markReserved()) {
                log.info("중복 재고 예약 완료 이벤트 무시: orderId={}", orderId);
            }
        } catch (IllegalStateException e) {
            log.warn("모순된 재고 예약 완료 이벤트 무시: {}", e.getMessage());
        }
    }

    /** 재고 예약 실패 이벤트 처리(보상): PENDING → CANCELLED. 중복과 모순 처리는 markReserved와 같다. */
    @Transactional
    public void cancel(Long orderId) {
        Order order = findOrder(orderId);
        try {
            if (!order.cancel()) {
                log.info("중복 재고 예약 실패 이벤트 무시: orderId={}", orderId);
            }
        } catch (IllegalStateException e) {
            log.warn("모순된 재고 예약 실패 이벤트 무시: {}", e.getMessage());
        }
    }

    private Order findOrder(Long orderId) {
        return orderRepository.findById(orderId).orElseThrow(() -> new OrderNotFoundException(orderId));
    }

    private OrderResult create(String idempotencyKey, String fingerprint, PlaceOrderCommand command, boolean keyHeld) {
        Order order;
        try {
            // 커밋까지 끝나야 반환된다
            order = transactionTemplate.execute(status -> {
                Order saved = orderRepository.save(
                        Order.place(idempotencyKey, fingerprint, command.productId(), command.quantity()));
                // 트랜잭션 안에서 발행하지만, Kafka 전송은 커밋된 뒤에 일어난다 (D-025)
                eventPublisher.publishEvent(new OrderPlacedEvent(saved.getId(), saved.getProductId(), saved.getQuantity()));
                return saved;
            });
        } catch (DataIntegrityViolationException e) {
            // DB 최종 방어선: 같은 키의 주문이 이미 커밋돼 있다 (Redis 키 만료/유실/장애로 1단계를 통과한 경우)
            Order existing = orderRepository.findByIdempotencyKey(idempotencyKey).orElseThrow(() -> e);
            if (!existing.getRequestFingerprint().equals(fingerprint)) {
                releaseQuietly(keyHeld, idempotencyKey, fingerprint);
                throw new IdempotencyKeyReusedException(idempotencyKey);
            }
            completeQuietly(keyHeld, idempotencyKey, fingerprint, existing.getId());
            return OrderResult.replayed(existing);
        } catch (RuntimeException e) {
            // 저장 실패 → 선점을 풀어 같은 키로 재시도할 수 있게 한다
            releaseQuietly(keyHeld, idempotencyKey, fingerprint);
            throw e;
        }
        completeQuietly(keyHeld, idempotencyKey, fingerprint, order.getId());
        return OrderResult.created(order);
    }

    private static void requireSameRequest(String idempotencyKey, String storedFingerprint, String fingerprint) {
        if (!storedFingerprint.equals(fingerprint)) {
            throw new IdempotencyKeyReusedException(idempotencyKey);
        }
    }

    /**
     * 주문은 이미 커밋됐으므로 Redis 기록 실패로 요청을 실패시키지 않는다.
     * 기록이 빠져도 처리 중 키는 30초 뒤 만료되고, 그 뒤의 재요청은 DB 제약이 막아 같은 주문을 돌려준다.
     */
    private void completeQuietly(boolean keyHeld, String idempotencyKey, String fingerprint, long orderId) {
        if (!keyHeld) {
            return;
        }
        try {
            idempotencyStore.complete(idempotencyKey, fingerprint, orderId);
        } catch (DataAccessException e) {
            log.warn("Redis 멱등 키 완료 기록 실패: key={}, orderId={}", idempotencyKey, orderId, e);
        }
    }

    private void releaseQuietly(boolean keyHeld, String idempotencyKey, String fingerprint) {
        if (!keyHeld) {
            return;
        }
        try {
            idempotencyStore.release(idempotencyKey, fingerprint);
        } catch (DataAccessException e) {
            log.warn("Redis 멱등 키 해제 실패 (30초 뒤 자동 만료): key={}", idempotencyKey, e);
        }
    }
}
