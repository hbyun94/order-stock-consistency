package io.github.hbyun94.stock.messaging;

import io.github.hbyun94.stock.application.StockNotFoundException;
import io.github.hbyun94.stock.application.reservation.PessimisticLockStockReservationStrategy;
import io.github.hbyun94.stock.domain.InsufficientStockException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * order.created를 받아 재고를 예약하고, 결과를 stock.reserved 또는 stock.reservation-failed로 보낸다.
 * <p>
 * 처리 순서: 예약(비관적 락, D-024) 트랜잭션 커밋 → 결과 이벤트 전송 → 전송 확인 → 메서드 정상 종료 → 오프셋 커밋.
 * <ul>
 *   <li>업무 실패(재고 부족, 미등록 상품)는 예외가 아니라 실패 이벤트로 알린다. 주문 쪽이 보상(취소)한다 (D-030).</li>
 *   <li>기술 실패(DB 장애, 결과 전송 실패)는 예외를 그대로 던진다. 오프셋이 커밋되지 않아 Spring Kafka가 다시 전달한다.</li>
 * </ul>
 * <b>알려진 약점:</b>
 * <ul>
 *   <li>예약 커밋 후 결과 전송이 실패하면 메시지가 다시 전달되는데, 같은 주문을 또 예약하려다
 *       order_id UNIQUE 제약에 막혀 계속 실패한다. 같은 이벤트를 두 번 처리하는 문제는 6단계에서 다룬다.</li>
 *   <li>커밋 후 전송 실패로 결과 이벤트가 유실될 수 있는 문제는 7단계 Outbox에서 다룬다 (D-029).</li>
 * </ul>
 */
@Component
public class OrderCreatedListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedListener.class);
    private static final long SEND_TIMEOUT_SECONDS = 10;

    private final PessimisticLockStockReservationStrategy reservation;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderCreatedListener(
            PessimisticLockStockReservationStrategy reservation,
            KafkaTemplate<String, Object> kafkaTemplate) {
        this.reservation = reservation;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = KafkaTopics.ORDER_CREATED)
    public void onOrderCreated(OrderCreatedEvent event) {
        String topic;
        Object result;
        try {
            // @Transactional 메서드: 반환되면 예약이 커밋된 상태다
            reservation.reserve(event.orderId(), event.productId(), event.quantity());
            topic = KafkaTopics.STOCK_RESERVED;
            result = new StockReservedEvent(event.orderId(), event.productId(), event.quantity());
        } catch (InsufficientStockException e) {
            topic = KafkaTopics.STOCK_RESERVATION_FAILED;
            result = failed(event, ReservationFailureReason.INSUFFICIENT_STOCK);
        } catch (StockNotFoundException e) {
            topic = KafkaTopics.STOCK_RESERVATION_FAILED;
            result = failed(event, ReservationFailureReason.UNKNOWN_PRODUCT);
        }
        log.info("재고 예약 결과: orderId={}, topic={}", event.orderId(), topic);
        sendAndWait(topic, String.valueOf(event.orderId()), result);
    }

    private static StockReservationFailedEvent failed(OrderCreatedEvent event, ReservationFailureReason reason) {
        return new StockReservationFailedEvent(event.orderId(), event.productId(), event.quantity(), reason);
    }

    /**
     * 브로커가 받았다고 확인할 때까지 기다린다. 실패하면 예외를 던져 이 메시지를 다시 처리하게 한다.
     * 결과 이벤트의 키는 주문 ID다: 같은 주문의 결과는 같은 파티션으로 간다 (D-027).
     */
    private void sendAndWait(String topic, String key, Object payload) {
        try {
            kafkaTemplate.send(topic, key, payload).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("결과 이벤트 전송 대기 중 인터럽트: " + topic, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("결과 이벤트 전송 실패: " + topic + ", key=" + key, e);
        }
    }
}
