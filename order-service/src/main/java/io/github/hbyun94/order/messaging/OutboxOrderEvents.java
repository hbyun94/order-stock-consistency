package io.github.hbyun94.order.messaging;

import io.github.hbyun94.order.application.OrderEvents;
import io.github.hbyun94.order.domain.Order;
import io.github.hbyun94.order.outbox.OutboxEvent;
import io.github.hbyun94.order.outbox.OutboxEventRepository;
import java.time.Instant;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

/**
 * order.created를 Kafka로 바로 보내지 않고 Outbox 테이블에 저장한다 (D-036).
 * <p>
 * 5단계에서는 커밋 후 바로 Kafka로 보냈다(@TransactionalEventListener AFTER_COMMIT).
 * 그 방식은 커밋과 전송 사이에 브로커 장애나 프로세스 종료가 끼면 이벤트가 사라졌다 (실험 3).
 * 이제 주문과 같은 트랜잭션에 "보낼 메시지"를 남기고, 실제 전송은 OutboxRelay가 한다.
 */
@Component
public class OutboxOrderEvents implements OrderEvents {

    private final OutboxEventRepository outboxRepository;
    private final JsonMapper jsonMapper;

    public OutboxOrderEvents(OutboxEventRepository outboxRepository, JsonMapper jsonMapper) {
        this.outboxRepository = outboxRepository;
        this.jsonMapper = jsonMapper;
    }

    /** MANDATORY: 트랜잭션 밖에서 부르면 예외. 주문 저장 트랜잭션에 반드시 합류하게 강제한다. */
    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void orderPlaced(Order order) {
        OrderCreatedEvent event = new OrderCreatedEvent(
                order.getId(), order.getProductId(), order.getQuantity(), Instant.now());
        // 키 = 상품 ID: 같은 상품의 주문은 같은 파티션으로 간다 (D-027)
        outboxRepository.save(OutboxEvent.of(
                KafkaTopics.ORDER_CREATED, String.valueOf(order.getProductId()), jsonMapper.writeValueAsString(event)));
    }
}
