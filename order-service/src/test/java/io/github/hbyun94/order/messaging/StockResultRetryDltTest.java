package io.github.hbyun94.order.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.github.hbyun94.order.TestcontainersConfiguration;
import io.github.hbyun94.order.application.OrderNotFoundException;
import io.github.hbyun94.order.application.OrderService;
import io.github.hbyun94.order.application.PlaceOrderCommand;
import io.github.hbyun94.order.domain.OrderRepository;
import io.github.hbyun94.order.domain.OrderStatus;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
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
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

/**
 * 결과 이벤트 처리 실패 시의 재시도와 DLT(D-042 ~ D-045)를 실제 Kafka와 PostgreSQL에서 검증한다.
 * 테스트 시간을 줄이려고 재시도 간격만 짧게 바꾼다. 재시도 횟수(4회)는 운영 설정과 같다.
 */
@SpringBootTest(properties = {
        "kafka.retry.initial-interval-ms=100",
        "kafka.retry.max-interval-ms=400"
})
@Import(TestcontainersConfiguration.class)
class StockResultRetryDltTest {

    private static final Duration WAIT = Duration.ofSeconds(30);
    private static final int MAX_ATTEMPTS = 5; // 최초 1회 + 재시도 4회 (D-043)

    @MockitoSpyBean
    OrderService orderService;

    @Autowired
    OrderRepository orderRepository;

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    ConsumerFactory<String, String> consumerFactory;

    Consumer<String, String> dltConsumer;

    @BeforeEach
    void setUp() {
        dltConsumer = consumerFactory.createConsumer("test-dlt-" + UUID.randomUUID(), null);
        dltConsumer.subscribe(List.of(KafkaTopics.STOCK_RESERVED_DLT, KafkaTopics.STOCK_RESERVATION_FAILED_DLT));
    }

    @AfterEach
    void tearDown() {
        dltConsumer.close();
    }

    @Test
    @DisplayName("없는 주문의 결과 이벤트는 재시도 없이 바로 stock.reserved.dlt로 간다")
    void unknownOrderGoesStraightToDlt() {
        long unknownOrderId = ThreadLocalRandom.current().nextLong(1_000_000_000L, Long.MAX_VALUE);

        kafkaTemplate.send(KafkaTopics.STOCK_RESERVED, String.valueOf(unknownOrderId),
                new StockReservedEvent(unknownOrderId, 1L, 2)).join();

        ConsumerRecord<String, String> dead = awaitDeadLetter(String.valueOf(unknownOrderId));
        assertThat(dead.topic()).isEqualTo(KafkaTopics.STOCK_RESERVED_DLT);
        assertThat(header(dead, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo(KafkaTopics.STOCK_RESERVED);
        assertThat(header(dead, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).isEqualTo(OrderNotFoundException.class.getName());
        verify(orderService, times(1)).markReserved(unknownOrderId);
    }

    @Test
    @DisplayName("보상 처리가 계속 실패하면 5번 시도 후 stock.reservation-failed.dlt로 가고, 주문은 PENDING에 남는다")
    void persistentFailureGoesToDltAfterRetries() {
        Long orderId = placePendingOrder();
        doThrow(new DataAccessResourceFailureException("DB 장애 지속 흉내")).when(orderService).cancel(eq(orderId));

        kafkaTemplate.send(KafkaTopics.STOCK_RESERVATION_FAILED, String.valueOf(orderId),
                new StockReservationFailedEvent(orderId, 1L, 2, "INSUFFICIENT_STOCK")).join();

        ConsumerRecord<String, String> dead = awaitDeadLetter(String.valueOf(orderId));
        assertThat(dead.topic()).isEqualTo(KafkaTopics.STOCK_RESERVATION_FAILED_DLT);
        assertThat(dead.value()).contains("\"orderId\":" + orderId); // 원문 JSON 그대로
        assertThat(header(dead, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN))
                .isEqualTo(DataAccessResourceFailureException.class.getName());
        verify(orderService, times(MAX_ATTEMPTS)).cancel(orderId);
        // 처리되지 못했으므로 주문은 그대로다. DLT 메시지를 보고 사람이 재처리한다 (D-046)
        assertThat(orderRepository.findById(orderId).orElseThrow().getStatus()).isEqualTo(OrderStatus.PENDING);
    }

    private Long placePendingOrder() {
        return orderService.placeOrder(UUID.randomUUID().toString(), new PlaceOrderCommand(1L, 2)).order().getId();
    }

    private ConsumerRecord<String, String> awaitDeadLetter(String key) {
        List<ConsumerRecord<String, String>> seen = new ArrayList<>();
        await().atMost(WAIT).until(() -> {
            dltConsumer.poll(Duration.ofMillis(500)).forEach(seen::add);
            return seen.stream().anyMatch(r -> key.equals(r.key()));
        });
        return seen.stream().filter(r -> key.equals(r.key())).findFirst().orElseThrow();
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
