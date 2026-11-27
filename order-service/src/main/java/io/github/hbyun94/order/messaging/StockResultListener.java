package io.github.hbyun94.order.messaging;

import io.github.hbyun94.order.application.OrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * stock-service의 재고 예약 결과를 받아 주문 상태를 바꾼다.
 * <ul>
 *   <li>stock.reserved → 주문 RESERVED</li>
 *   <li>stock.reservation-failed → 주문 CANCELLED (보상 처리, D-030)</li>
 * </ul>
 * 같은 결과 이벤트가 다시 와도(중복 소비) 상태 기반으로 무시한다. 예외를 던지지 않으므로 재시도가 반복되지 않는다 (D-034).
 * <p>
 * 주문 ID가 없거나 없는 주문이면 재시도 없이 DLT로, DB 장애 같은 기술 실패는 재시도 후 DLT로 보낸다 (D-042 ~ D-044).
 */
@Component
public class StockResultListener {

    private static final Logger log = LoggerFactory.getLogger(StockResultListener.class);

    private final OrderService orderService;

    public StockResultListener(OrderService orderService) {
        this.orderService = orderService;
    }

    @KafkaListener(topics = KafkaTopics.STOCK_RESERVED)
    public void onReserved(StockReservedEvent event) {
        requireOrderId(event.orderId(), event);
        orderService.markReserved(event.orderId());
        log.info("주문 재고 예약 완료: orderId={}", event.orderId());
    }

    @KafkaListener(topics = KafkaTopics.STOCK_RESERVATION_FAILED)
    public void onReservationFailed(StockReservationFailedEvent event) {
        requireOrderId(event.orderId(), event);
        orderService.cancel(event.orderId());
        log.info("재고 예약 실패로 주문 취소: orderId={}, reason={}", event.orderId(), event.reason());
    }

    /** 주문 ID가 없으면 재시도해도 소용없으므로 바로 DLT로 보낸다 (D-044) */
    private static void requireOrderId(Long orderId, Object event) {
        if (orderId == null) {
            throw new InvalidEventException("결과 이벤트에 주문 ID가 없음: " + event);
        }
    }
}
