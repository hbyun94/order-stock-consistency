package io.github.hbyun94.stock.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.github.hbyun94.stock.TestcontainersConfiguration;
import io.github.hbyun94.stock.application.OrderReservationService;
import io.github.hbyun94.stock.domain.OrderEventResultRepository;
import io.github.hbyun94.stock.domain.ReservationOutcome;
import io.github.hbyun94.stock.domain.Stock;
import io.github.hbyun94.stock.domain.StockRepository;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

/**
 * order.created 처리 실패 시의 재시도와 DLT(D-042 ~ D-045)를 실제 Kafka와 PostgreSQL에서 검증한다.
 * <p>
 * 테스트 시간을 줄이려고 재시도 간격만 짧게 바꾼다(100ms부터 2배, 최대 400ms). 재시도 횟수(4회)는 운영 설정과 같다.
 * 일시 장애와 계속되는 장애는 예약 서비스(스파이)가 지정한 횟수만큼 예외를 던지게 해서 흉내 낸다.
 */
@SpringBootTest(properties = {
        "kafka.retry.initial-interval-ms=100",
        "kafka.retry.max-interval-ms=400"
})
@Import(TestcontainersConfiguration.class)
class OrderCreatedRetryDltTest {

    private static final Duration WAIT = Duration.ofSeconds(30);
    private static final int MAX_ATTEMPTS = 5; // 최초 1회 + 재시도 4회 (D-043)

    @MockitoSpyBean
    OrderReservationService reservationService;

    @Autowired
    ProducerFactory<?, ?> producerFactory;

    @Autowired
    ConsumerFactory<String, String> consumerFactory;

    @Autowired
    StockRepository stockRepository;

    @Autowired
    OrderEventResultRepository resultRepository;

    KafkaTemplate<String, String> rawTemplate;
    Consumer<String, String> dltConsumer;
    long productId;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        // 원문 문자열을 그대로 보내는 템플릿: 깨진 JSON도 보낼 수 있다
        rawTemplate = new KafkaTemplate<>((ProducerFactory<String, String>) producerFactory.copyWithConfigurationOverride(
                Map.of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)));
        dltConsumer = consumerFactory.createConsumer("test-dlt-" + UUID.randomUUID(), null);
        dltConsumer.subscribe(List.of(KafkaTopics.ORDER_CREATED_DLT));

        productId = ThreadLocalRandom.current().nextLong(1, 1_000_000);
        stockRepository.save(Stock.create(productId, 10));
    }

    @AfterEach
    void tearDown() {
        dltConsumer.close();
        rawTemplate.destroy();
    }

    @Test
    @DisplayName("깨진 JSON은 재시도 없이 바로 order.created.dlt로 가고, 원문과 실패 원인이 남는다")
    void malformedJsonGoesStraightToDlt() throws Exception {
        String key = "malformed-" + UUID.randomUUID();
        String body = "{\"orderId\": 1, \"productId\": "; // 끊긴 JSON
        rawTemplate.send(KafkaTopics.ORDER_CREATED, key, body).get();

        ConsumerRecord<String, String> dead = awaitDeadLetter(key);

        assertThat(dead.value()).isEqualTo(body); // 원문 그대로 (따옴표로 다시 감싸지 않음)
        assertThat(header(dead, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo(KafkaTopics.ORDER_CREATED);
        assertThat(header(dead, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).contains("Conversion");
        // 변환 단계에서 실패했으므로 예약 서비스는 한 번도 불리지 않았다
        verify(reservationService, never()).process(anyLong(), anyLong(), anyInt());
    }

    @Test
    @DisplayName("필수 값이 빠진 이벤트는 재시도 없이 바로 DLT로 간다")
    void missingFieldGoesStraightToDlt() throws Exception {
        String key = String.valueOf(productId);
        String body = "{\"productId\": " + productId + ", \"quantity\": 1}"; // orderId 없음
        rawTemplate.send(KafkaTopics.ORDER_CREATED, key, body).get();

        ConsumerRecord<String, String> dead = awaitDeadLetter(key);

        assertThat(dead.value()).isEqualTo(body);
        assertThat(header(dead, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).isEqualTo(InvalidEventException.class.getName());
        verify(reservationService, never()).process(anyLong(), anyLong(), anyInt());
    }

    @Test
    @DisplayName("일시 장애(2번 실패 후 회복)는 재시도로 정상 처리되고 DLT로 가지 않는다")
    void transientFailureRecoversByRetry() throws Exception {
        long orderId = newOrderId();
        doThrow(new DataAccessResourceFailureException("DB 일시 장애 흉내"))
                .doThrow(new DataAccessResourceFailureException("DB 일시 장애 흉내"))
                .doCallRealMethod()
                .when(reservationService).process(eq(orderId), anyLong(), anyInt());

        sendOrderCreated(orderId, 1);

        await().atMost(WAIT).until(() -> resultRepository.findById(orderId).isPresent());
        assertThat(resultRepository.findById(orderId).orElseThrow().getOutcome()).isEqualTo(ReservationOutcome.RESERVED);
        verify(reservationService, times(3)).process(eq(orderId), anyLong(), anyInt());
        assertThat(stockRepository.findByProductId(productId).orElseThrow().getReserved()).isEqualTo(1);
        assertThat(pollDeadLetters(Duration.ofSeconds(2))).noneMatch(r -> r.value().contains(String.valueOf(orderId)));
    }

    @Test
    @DisplayName("계속 실패하면 5번 시도 후 DLT로 보내고, 같은 파티션의 다음 메시지는 정상 처리한다")
    void persistentFailureGoesToDltAfterRetriesAndNextMessageProceeds() throws Exception {
        long failingOrderId = newOrderId();
        long nextOrderId = newOrderId();
        doThrow(new DataAccessResourceFailureException("DB 장애 지속 흉내"))
                .when(reservationService).process(eq(failingOrderId), anyLong(), anyInt());

        // 같은 상품 키 → 같은 파티션. 앞 메시지가 막혀 있는 동안 뒤 메시지는 기다린다 (순서 유지, D-042)
        sendOrderCreated(failingOrderId, 1);
        sendOrderCreated(nextOrderId, 1);

        ConsumerRecord<String, String> dead = awaitDeadLetter(String.valueOf(productId));
        assertThat(dead.value()).contains("\"orderId\":" + failingOrderId);
        assertThat(header(dead, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN))
                .isEqualTo(DataAccessResourceFailureException.class.getName());
        verify(reservationService, times(MAX_ATTEMPTS)).process(eq(failingOrderId), anyLong(), anyInt());

        await().atMost(WAIT).until(() -> resultRepository.findById(nextOrderId).isPresent());
        assertThat(resultRepository.findById(failingOrderId)).isEmpty();
        assertThat(stockRepository.findByProductId(productId).orElseThrow().getReserved()).isEqualTo(1);
        verify(reservationService, timeout(1_000).times(1)).process(eq(nextOrderId), anyLong(), anyInt());
    }

    private void sendOrderCreated(long orderId, int quantity) throws Exception {
        String body = "{\"orderId\":" + orderId + ",\"productId\":" + productId + ",\"quantity\":" + quantity
                + ",\"occurredAt\":\"2026-11-27T00:00:00Z\"}";
        rawTemplate.send(KafkaTopics.ORDER_CREATED, String.valueOf(productId), body).get();
    }

    private ConsumerRecord<String, String> awaitDeadLetter(String key) {
        List<ConsumerRecord<String, String>> seen = new ArrayList<>();
        await().atMost(WAIT).until(() -> {
            seen.addAll(pollDeadLetters(Duration.ofMillis(500)));
            return seen.stream().anyMatch(r -> key.equals(r.key()));
        });
        return seen.stream().filter(r -> key.equals(r.key())).findFirst().orElseThrow();
    }

    private List<ConsumerRecord<String, String>> pollDeadLetters(Duration duration) {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        dltConsumer.poll(duration).forEach(records::add);
        return records;
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static long newOrderId() {
        return ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
    }
}
