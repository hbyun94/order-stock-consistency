package io.github.hbyun94.stock.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.hbyun94.stock.TestcontainersConfiguration;
import io.github.hbyun94.stock.application.OrderReservationService;
import io.github.hbyun94.stock.domain.Stock;
import io.github.hbyun94.stock.domain.StockRepository;
import io.github.hbyun94.stock.outbox.OutboxEvent;
import io.github.hbyun94.stock.outbox.OutboxEventRepository;
import io.github.hbyun94.stock.outbox.OutboxRelay;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.ConsumerFactory;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.json.JsonMapper;

/**
 * 결과 이벤트 전송 실패(실제 브로커 장애)에도 결과가 유실되지 않고, 재고는 한 번만 차감되는지 검증한다.
 * <p>
 * 6단계의 OrderCreatedRedeliveryTest를 대체한다. 그때는 리스너가 결과를 직접 보냈고, 전송 실패 시
 * 메시지 재전달 → 기록된 결과 재발행으로 복구했다. 이제는 결과가 결정과 같은 트랜잭션으로 Outbox에 저장되고,
 * 발행기가 브로커 복구 후 다시 보낸다 (D-036). 같은 의도(유실 없음, 재고 1회 차감)를 실제 브로커 장애로 검증한다.
 */
@SpringBootTest(properties = {
        "outbox.relay.interval-ms=3600000",
        "outbox.relay.send-timeout-ms=2000"
})
@Import(TestcontainersConfiguration.class)
class ResultEventOutageTest {

    @Autowired
    OrderReservationService reservationService;

    @Autowired
    OutboxRelay relay;

    @Autowired
    OutboxEventRepository outboxRepository;

    @Autowired
    StockRepository stockRepository;

    @Autowired
    ConsumerFactory<String, String> consumerFactory;

    @Autowired
    JsonMapper jsonMapper;

    @Autowired
    KafkaContainer kafka;

    @Test
    @DisplayName("결과 전송 중 브로커가 죽어도 결과는 Outbox에 남아 복구 후 발행되고, 재고는 한 번만 차감된다")
    void resultSurvivesBrokerOutage() {
        relay.relay(); // 이전 테스트가 남긴 행 정리
        long productId = ThreadLocalRandom.current().nextLong(1, 1_000_000);
        long orderId = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
        stockRepository.save(Stock.create(productId, 5));

        reservationService.process(orderId, productId, 3); // 결정 + Outbox 저장 커밋

        pauseKafka();
        try {
            assertThat(relay.relay()).isZero(); // 브로커 장애: 발행 실패
            OutboxEvent pending = findResultRow(orderId);
            assertThat(pending.getPublishedAt()).isNull();
            assertThat(pending.getAttempts()).isEqualTo(1);

            // 장애 중에 같은 order.created가 다시 와도(재전달) 재고는 다시 차감되지 않는다
            assertThat(reservationService.process(orderId, productId, 3).duplicate()).isTrue();
        } finally {
            unpauseKafka();
        }

        assertThat(relay.relay()).isGreaterThanOrEqualTo(1);
        assertThat(findResultRow(orderId).getPublishedAt()).isNotNull();
        assertThat(stockRepository.findByProductId(productId).orElseThrow().getReserved()).isEqualTo(3);

        try (Consumer<String, String> consumer = consumerFactory.createConsumer("test-" + UUID.randomUUID(), null)) {
            consumer.subscribe(List.of(KafkaTopics.STOCK_RESERVED));
            await().atMost(Duration.ofSeconds(15)).until(() -> {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
                    if (jsonMapper.readTree(record.value()).get("orderId").asLong() == orderId) {
                        return true;
                    }
                }
                return false;
            });
        }
    }

    private OutboxEvent findResultRow(long orderId) {
        return outboxRepository.findAll().stream()
                .filter(e -> jsonMapper.readTree(e.getPayload()).get("orderId").asLong() == orderId)
                .findFirst()
                .orElseThrow();
    }

    private void pauseKafka() {
        kafka.getDockerClient().pauseContainerCmd(kafka.getContainerId()).exec();
    }

    private void unpauseKafka() {
        kafka.getDockerClient().unpauseContainerCmd(kafka.getContainerId()).exec();
    }
}
