package io.github.hbyun94.stock.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hbyun94.stock.TestcontainersConfiguration;
import io.github.hbyun94.stock.domain.OrderEventResultRepository;
import io.github.hbyun94.stock.domain.ReservationStatus;
import io.github.hbyun94.stock.domain.Stock;
import io.github.hbyun94.stock.domain.StockRepository;
import io.github.hbyun94.stock.domain.StockReservationRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * 실제 Kafka + PostgreSQL에서 order.created 소비 → 재고 예약 → 결과 이벤트 발행을 검증한다.
 * order-service 대신 테스트가 order.created를 직접 보낸다 (JSON 필드 계약만 맞으면 된다, D-028).
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class OrderCreatedListenerTest {

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

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
    Long productId;

    @BeforeEach
    void setUp() {
        resultRepository.deleteAllInBatch();
        reservationRepository.deleteAllInBatch();
        stockRepository.deleteAllInBatch();
        productId = ThreadLocalRandom.current().nextLong(1, 1_000_000);
        stockRepository.save(Stock.create(productId, 2));

        resultConsumer = consumerFactory.createConsumer("test-" + UUID.randomUUID(), null);
        resultConsumer.subscribe(List.of(KafkaTopics.STOCK_RESERVED, KafkaTopics.STOCK_RESERVATION_FAILED));
    }

    @AfterEach
    void close() {
        resultConsumer.close();
    }

    @Test
    @DisplayName("재고가 충분하면 예약하고 stock.reserved를 주문 ID 키로 보낸다")
    void reserved() {
        long orderId = newOrderId();

        sendOrderCreated(orderId, productId, 2);

        ConsumerRecord<String, String> result = awaitResult(orderId);
        assertThat(result.topic()).isEqualTo(KafkaTopics.STOCK_RESERVED);
        assertThat(result.key()).isEqualTo(String.valueOf(orderId));
        assertThat(json(result).get("quantity").asInt()).isEqualTo(2);
        assertThat(reservationRepository.findByOrderId(orderId))
                .hasValueSatisfying(r -> assertThat(r.getStatus()).isEqualTo(ReservationStatus.RESERVED));
        assertThat(stockRepository.findByProductId(productId).orElseThrow().getReserved()).isEqualTo(2);
    }

    @Test
    @DisplayName("재고가 부족하면 아무것도 예약하지 않고 stock.reservation-failed(INSUFFICIENT_STOCK)를 보낸다")
    void insufficientStock() {
        long orderId = newOrderId();

        sendOrderCreated(orderId, productId, 3);

        ConsumerRecord<String, String> result = awaitResult(orderId);
        assertThat(result.topic()).isEqualTo(KafkaTopics.STOCK_RESERVATION_FAILED);
        assertThat(json(result).get("reason").asString()).isEqualTo("INSUFFICIENT_STOCK");
        assertThat(reservationRepository.findByOrderId(orderId)).isEmpty();
        assertThat(stockRepository.findByProductId(productId).orElseThrow().getReserved()).isZero();
    }

    @Test
    @DisplayName("재고가 등록되지 않은 상품이면 stock.reservation-failed(UNKNOWN_PRODUCT)를 보낸다")
    void unknownProduct() {
        long orderId = newOrderId();

        sendOrderCreated(orderId, productId + 1, 1);

        ConsumerRecord<String, String> result = awaitResult(orderId);
        assertThat(result.topic()).isEqualTo(KafkaTopics.STOCK_RESERVATION_FAILED);
        assertThat(json(result).get("reason").asString()).isEqualTo("UNKNOWN_PRODUCT");
    }

    @Test
    @DisplayName("[중복 소비] 같은 order.created가 두 번 오면 재고는 한 번만 예약하고 결과 이벤트도 한 번만 나간다 (D-039)")
    void duplicateOrderCreated() {
        long orderId = newOrderId();

        sendOrderCreated(orderId, productId, 2);
        sendOrderCreated(orderId, productId, 2);

        List<ConsumerRecord<String, String>> results = awaitResults(orderId, 1);
        assertThat(results).extracting(ConsumerRecord::topic).containsOnly(KafkaTopics.STOCK_RESERVED);
        assertNoMoreResults(orderId, Duration.ofSeconds(3));
        assertThat(stockRepository.findByProductId(productId).orElseThrow().getReserved()).isEqualTo(2);
        assertThat(reservationRepository.count()).isEqualTo(1);
        assertThat(resultRepository.count()).isEqualTo(1);
    }

    private void sendOrderCreated(long orderId, long productId, int quantity) {
        kafkaTemplate.send(KafkaTopics.ORDER_CREATED, String.valueOf(productId),
                new OrderCreatedEvent(orderId, productId, quantity, Instant.now())).join();
    }

    /** 이 주문 ID의 결과 이벤트가 올 때까지 기다린다 (최대 15초) */
    private ConsumerRecord<String, String> awaitResult(long orderId) {
        return awaitResults(orderId, 1).getFirst();
    }

    /** 이 주문 ID의 결과 이벤트를 count건 받을 때까지 기다린다 (최대 15초) */
    private List<ConsumerRecord<String, String>> awaitResults(long orderId, int count) {
        List<ConsumerRecord<String, String>> matched = new ArrayList<>();
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline && matched.size() < count) {
            for (ConsumerRecord<String, String> record : resultConsumer.poll(Duration.ofMillis(200))) {
                if (json(record).get("orderId").asLong() == orderId) {
                    matched.add(record);
                }
            }
        }
        if (matched.size() < count) {
            throw new AssertionError("결과 이벤트를 " + count + "건 받지 못했습니다: orderId=" + orderId + ", 받은 수=" + matched.size());
        }
        return matched;
    }

    /** 일정 시간 동안 이 주문의 결과 이벤트가 더 오지 않는지 확인한다 */
    private void assertNoMoreResults(long orderId, Duration wait) {
        long deadline = System.nanoTime() + wait.toNanos();
        while (System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> record : resultConsumer.poll(Duration.ofMillis(200))) {
                if (json(record).get("orderId").asLong() == orderId) {
                    throw new AssertionError("결과 이벤트가 한 번 더 왔습니다: orderId=" + orderId);
                }
            }
        }
    }

    private JsonNode json(ConsumerRecord<String, String> record) {
        return jsonMapper.readTree(record.value());
    }

    private static long newOrderId() {
        return ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
    }
}
