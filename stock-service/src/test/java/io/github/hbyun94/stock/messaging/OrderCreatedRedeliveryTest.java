package io.github.hbyun94.stock.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;

import io.github.hbyun94.stock.TestcontainersConfiguration;
import io.github.hbyun94.stock.domain.OrderEventResultRepository;
import io.github.hbyun94.stock.domain.Stock;
import io.github.hbyun94.stock.domain.StockRepository;
import io.github.hbyun94.stock.domain.StockReservationRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import tools.jackson.databind.json.JsonMapper;

/**
 * 실제 재전달 경로를 재현한다: 예약은 커밋됐는데 결과 이벤트 전송이 한 번 실패한 경우.
 * <p>
 * 리스너가 예외를 던지면 오프셋이 커밋되지 않고 Spring Kafka가 같은 메시지를 다시 전달한다.
 * 5단계 코드에서는 이 재전달이 order_id UNIQUE에 막혀 계속 실패하고 주문이 PENDING에 멈췄다.
 * 멱등 처리(D-033) 후에는 기록된 결과를 다시 보내므로 결과 이벤트가 정상적으로 나간다.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class OrderCreatedRedeliveryTest {

    // Boot는 KafkaTemplate을 KafkaTemplate<?, ?> 타입 빈으로 등록하므로, 스파이 필드는 raw 타입으로 선언해야 매칭된다
    @SuppressWarnings("rawtypes")
    @MockitoSpyBean
    KafkaTemplate kafkaTemplate;

    @Autowired
    ConsumerFactory<String, String> consumerFactory;

    @Autowired
    JsonMapper jsonMapper;

    @Autowired
    StockRepository stockRepository;

    @Autowired
    StockReservationRepository reservationRepository;

    @Autowired
    OrderEventResultRepository resultRepository;

    Consumer<String, String> resultConsumer;

    @AfterEach
    void close() {
        if (resultConsumer != null) {
            resultConsumer.close();
        }
    }

    @Test
    @DisplayName("결과 전송이 한 번 실패해도 재전달 때 재고를 다시 차감하지 않고 결과 이벤트를 보낸다")
    @SuppressWarnings("unchecked")
    void redeliveryAfterSendFailure() {
        long productId = ThreadLocalRandom.current().nextLong(1, 1_000_000);
        long orderId = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
        stockRepository.save(Stock.create(productId, 5));
        resultConsumer = consumerFactory.createConsumer("test-" + UUID.randomUUID(), null);
        resultConsumer.subscribe(List.of(KafkaTopics.STOCK_RESERVED));

        // stock.reserved 첫 전송만 실패시키고, 그다음부터는 실제로 보낸다
        Mockito.doReturn(CompletableFuture.failedFuture(new KafkaException("주입된 전송 실패")))
                .doCallRealMethod()
                .when(kafkaTemplate).send(eq(KafkaTopics.STOCK_RESERVED), anyString(), any());

        kafkaTemplate.send(KafkaTopics.ORDER_CREATED, String.valueOf(productId),
                new OrderCreatedEvent(orderId, productId, 3, Instant.now())).join();

        List<ConsumerRecord<String, String>> results = awaitResults(orderId);
        assertThat(results).hasSize(1);
        verify(kafkaTemplate, atLeast(2)).send(eq(KafkaTopics.STOCK_RESERVED), anyString(), any());
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(stockRepository.findByProductId(productId).orElseThrow().getReserved()).isEqualTo(3);
            assertThat(reservationRepository.findByOrderId(orderId)).isPresent();
            assertThat(resultRepository.findById(orderId)).isPresent();
        });
    }

    /** 이 주문의 stock.reserved를 모은다. 1건을 받은 뒤에도 잠시 더 기다려 추가로 오지 않는지 본다. */
    private List<ConsumerRecord<String, String>> awaitResults(long orderId) {
        List<ConsumerRecord<String, String>> matched = new ArrayList<>();
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        long settleUntil = Long.MAX_VALUE;
        while (System.nanoTime() < Math.min(deadline, settleUntil)) {
            for (ConsumerRecord<String, String> record : resultConsumer.poll(Duration.ofMillis(200))) {
                if (jsonMapper.readTree(record.value()).get("orderId").asLong() == orderId) {
                    matched.add(record);
                    settleUntil = System.nanoTime() + Duration.ofSeconds(2).toNanos();
                }
            }
        }
        return matched;
    }
}
