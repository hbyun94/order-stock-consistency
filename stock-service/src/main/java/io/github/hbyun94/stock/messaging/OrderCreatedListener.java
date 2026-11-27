package io.github.hbyun94.stock.messaging;

import io.github.hbyun94.stock.application.OrderReservationService;
import io.github.hbyun94.stock.application.OrderReservationService.Processed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * order.created를 받아 재고 예약을 결정한다.
 * <p>
 * 결정, 결정 기록, 결과 이벤트(Outbox) 저장이 한 트랜잭션으로 커밋되면 메서드가 끝나고 오프셋이 커밋된다.
 * 결과 이벤트의 실제 전송은 OutboxRelay가 맡는다 (D-036). 그래서 여기서는 Kafka 전송을 기다리지 않는다.
 * <ul>
 *   <li>업무 실패(재고 부족, 미등록 상품)는 거절 결정으로 기록되고 실패 이벤트가 나간다 (D-030).</li>
 *   <li>기술 실패(DB 장애, 락 대기 초과)는 예외를 그대로 던진다. 에러 핸들러가 간격을 두고 재시도하고,
 *       끝내 실패하면 order.created.dlt로 보낸다 (D-042, D-043).</li>
 *   <li>필수 값이 빠진 이벤트는 재시도 없이 바로 DLT로 보낸다 (D-044).</li>
 *   <li>중복 소비: 이미 결정된 주문이면 아무것도 하지 않는다. 첫 결정의 결과 이벤트는 Outbox가 발행을 보장한다 (D-039).</li>
 * </ul>
 */
@Component
public class OrderCreatedListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedListener.class);

    private final OrderReservationService reservationService;

    public OrderCreatedListener(OrderReservationService reservationService) {
        this.reservationService = reservationService;
    }

    @KafkaListener(topics = KafkaTopics.ORDER_CREATED)
    public void onOrderCreated(OrderCreatedEvent event) {
        validate(event);
        Processed processed = reservationService.process(event.orderId(), event.productId(), event.quantity());
        if (processed.duplicate()) {
            log.info("중복 order.created 무시 (결과는 이미 Outbox에 기록됨): orderId={}, outcome={}",
                    event.orderId(), processed.result().getOutcome());
        } else {
            log.info("재고 예약 결정: orderId={}, outcome={}", event.orderId(), processed.result().getOutcome());
        }
    }

    /** 필수 값이 없으면 재시도해도 소용없으므로 바로 DLT로 보낸다 (D-044) */
    private static void validate(OrderCreatedEvent event) {
        if (event.orderId() == null || event.productId() == null || event.quantity() <= 0) {
            throw new InvalidEventException("order.created 필수 값 누락 또는 잘못된 수량: " + event);
        }
    }
}
