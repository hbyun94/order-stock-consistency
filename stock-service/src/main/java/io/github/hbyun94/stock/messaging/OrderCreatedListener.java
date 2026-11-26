package io.github.hbyun94.stock.messaging;

import io.github.hbyun94.stock.application.OrderReservationService;
import io.github.hbyun94.stock.application.OrderReservationService.Processed;
import io.github.hbyun94.stock.domain.OrderEventResult;
import io.github.hbyun94.stock.domain.ReservationOutcome;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * order.created를 받아 재고 예약을 결정하고, 결과를 stock.reserved 또는 stock.reservation-failed로 보낸다.
 * <p>
 * 처리 순서: 결정과 기록(한 트랜잭션, 커밋) → 결과 이벤트 전송 → 브로커 확인 → 메서드 정상 종료 → 오프셋 커밋.
 * <ul>
 *   <li>업무 실패(재고 부족, 미등록 상품)는 예외가 아니라 실패 이벤트로 알린다 (D-030).</li>
 *   <li>기술 실패(DB 장애, 결과 전송 실패)는 예외를 그대로 던져 같은 메시지를 다시 처리하게 한다.</li>
 * </ul>
 * <b>중복 소비(D-033):</b> 같은 주문이 다시 오면 {@link OrderReservationService}가 새로 판단하지 않고
 * 기록된 결정을 돌려주고, 여기서는 그 결정으로 같은 결과 이벤트를 다시 보낸다.
 * 그래서 "예약은 커밋됐는데 결과 전송이 실패"한 경우에도 재전달 때 결과가 다시 나가 주문이 PENDING에 멈추지 않는다.
 * 결과 이벤트가 두 번 나갈 수 있으므로, 받는 쪽(order-service)도 멱등하게 처리한다 (D-034).
 * <p>
 * <b>남은 약점:</b> 커밋 후 전송이 실패하고 재전달마저 끝내 실패하면(재시도 소진) 결과가 유실된다. 7단계 Outbox에서 다룬다.
 */
@Component
public class OrderCreatedListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedListener.class);
    private static final long SEND_TIMEOUT_SECONDS = 10;

    private final OrderReservationService reservationService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderCreatedListener(OrderReservationService reservationService, KafkaTemplate<String, Object> kafkaTemplate) {
        this.reservationService = reservationService;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = KafkaTopics.ORDER_CREATED)
    public void onOrderCreated(OrderCreatedEvent event) {
        Processed processed = reservationService.process(event.orderId(), event.productId(), event.quantity());
        OrderEventResult result = processed.result();
        if (processed.duplicate()) {
            log.info("중복 order.created: 기록된 결과를 다시 보낸다: orderId={}, outcome={}",
                    event.orderId(), result.getOutcome());
        }

        String key = String.valueOf(result.getOrderId());
        if (result.getOutcome() == ReservationOutcome.RESERVED) {
            sendAndWait(KafkaTopics.STOCK_RESERVED, key,
                    new StockReservedEvent(result.getOrderId(), result.getProductId(), result.getQuantity()));
        } else {
            sendAndWait(KafkaTopics.STOCK_RESERVATION_FAILED, key,
                    new StockReservationFailedEvent(
                            result.getOrderId(), result.getProductId(), result.getQuantity(), result.getReason()));
        }
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
