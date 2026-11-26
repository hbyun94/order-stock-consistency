package io.github.hbyun94.order.messaging;

import io.github.hbyun94.order.application.OrderPlacedEvent;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * 주문 트랜잭션이 커밋된 뒤 order.created를 Kafka로 보낸다 (D-025).
 * <p>
 * AFTER_COMMIT이므로 롤백된 주문의 이벤트는 나가지 않는다.
 * <b>알려진 약점(의도적으로 남김):</b> 커밋은 됐는데 이 전송이 실패하면(브로커 장애, 프로세스 종료 등)
 * 이벤트가 사라지고, 주문은 PENDING에 영원히 멈춘다. DB 커밋과 Kafka 전송을 하나로 묶을 수 없기 때문이다.
 * 실험 3에서 이 상황을 재현하고, 7단계 Outbox 패턴으로 해결한다.
 */
@Component
public class OrderCreatedPublisher {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderCreatedPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void publish(OrderPlacedEvent placed) {
        OrderCreatedEvent event = new OrderCreatedEvent(
                placed.orderId(), placed.productId(), placed.quantity(), Instant.now());
        // 키 = 상품 ID: 같은 상품의 주문은 같은 파티션으로 가서 한 소비자가 순서대로 처리한다 (D-027)
        String key = String.valueOf(placed.productId());
        try {
            kafkaTemplate.send(KafkaTopics.ORDER_CREATED, key, event)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("order.created 전송 실패 — 이벤트 유실, 주문이 PENDING에 남는다: orderId={}",
                                    placed.orderId(), ex);
                        }
                    });
        } catch (RuntimeException e) {
            // 커밋이 끝난 뒤라 여기서 예외를 던져도 주문은 되돌릴 수 없다. 요청은 성공으로 두고 유실을 기록한다.
            log.error("order.created 전송 시작 실패 — 이벤트 유실, 주문이 PENDING에 남는다: orderId={}",
                    placed.orderId(), e);
        }
    }
}
