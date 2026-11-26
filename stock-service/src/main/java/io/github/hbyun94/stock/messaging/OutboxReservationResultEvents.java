package io.github.hbyun94.stock.messaging;

import io.github.hbyun94.stock.application.ReservationResultEvents;
import io.github.hbyun94.stock.domain.OrderEventResult;
import io.github.hbyun94.stock.domain.ReservationOutcome;
import io.github.hbyun94.stock.outbox.OutboxEvent;
import io.github.hbyun94.stock.outbox.OutboxEventRepository;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

/**
 * 재고 예약 결과를 Kafka로 바로 보내지 않고 Outbox 테이블에 저장한다 (D-036).
 * 결과 이벤트의 키는 주문 ID다: 같은 주문의 결과는 같은 파티션으로 간다 (D-027).
 */
@Component
public class OutboxReservationResultEvents implements ReservationResultEvents {

    private final OutboxEventRepository outboxRepository;
    private final JsonMapper jsonMapper;

    public OutboxReservationResultEvents(OutboxEventRepository outboxRepository, JsonMapper jsonMapper) {
        this.outboxRepository = outboxRepository;
        this.jsonMapper = jsonMapper;
    }

    /** MANDATORY: 트랜잭션 밖에서 부르면 예외. 결정 기록 트랜잭션에 반드시 합류하게 강제한다. */
    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void resultDecided(OrderEventResult result) {
        String key = String.valueOf(result.getOrderId());
        if (result.getOutcome() == ReservationOutcome.RESERVED) {
            save(KafkaTopics.STOCK_RESERVED, key,
                    new StockReservedEvent(result.getOrderId(), result.getProductId(), result.getQuantity()));
        } else {
            save(KafkaTopics.STOCK_RESERVATION_FAILED, key, new StockReservationFailedEvent(
                    result.getOrderId(), result.getProductId(), result.getQuantity(), result.getReason()));
        }
    }

    private void save(String topic, String key, Object event) {
        outboxRepository.save(OutboxEvent.of(topic, key, jsonMapper.writeValueAsString(event)));
    }
}
