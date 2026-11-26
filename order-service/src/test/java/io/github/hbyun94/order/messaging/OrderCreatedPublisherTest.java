package io.github.hbyun94.order.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hbyun94.order.TestcontainersConfiguration;
import io.github.hbyun94.order.application.OrderService;
import io.github.hbyun94.order.application.PlaceOrderCommand;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
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
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** 실제 Kafka에서 주문 생성 → order.created 발행을 검증한다 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class OrderCreatedPublisherTest {

    @Autowired
    OrderService orderService;

    @Autowired
    ConsumerFactory<String, String> consumerFactory;

    @Autowired
    JsonMapper jsonMapper;

    Consumer<String, String> consumer;

    @BeforeEach
    void subscribe() {
        // 테스트 전용 소비자 그룹으로 토픽을 처음부터 읽는다
        consumer = consumerFactory.createConsumer("test-" + UUID.randomUUID(), null);
        consumer.subscribe(List.of(KafkaTopics.ORDER_CREATED));
    }

    @AfterEach
    void close() {
        consumer.close();
    }

    @Test
    @DisplayName("주문이 저장되면 order.created가 상품 ID 키로 발행된다")
    void publishAfterCommit() {
        Long orderId = orderService.placeOrder(UUID.randomUUID().toString(), new PlaceOrderCommand(7L, 3))
                .order().getId();

        List<ConsumerRecord<String, String>> records = pollFor(orderId, Duration.ofSeconds(10), 1);

        assertThat(records).hasSize(1);
        ConsumerRecord<String, String> record = records.getFirst();
        assertThat(record.key()).isEqualTo("7");
        JsonNode body = jsonMapper.readTree(record.value());
        assertThat(body.get("orderId").asLong()).isEqualTo(orderId);
        assertThat(body.get("productId").asLong()).isEqualTo(7L);
        assertThat(body.get("quantity").asInt()).isEqualTo(3);
        assertThat(body.hasNonNull("occurredAt")).isTrue();
        // 타입 헤더를 끄므로 보내는 쪽 클래스 이름이 메시지에 실리지 않는다 (D-028)
        assertThat(record.headers().lastHeader("__TypeId__")).isNull();
    }

    @Test
    @DisplayName("같은 Idempotency-Key 재요청은 새 주문을 만들지 않으므로 이벤트도 다시 나가지 않는다")
    void replayDoesNotPublishAgain() {
        String key = UUID.randomUUID().toString();
        Long orderId = orderService.placeOrder(key, new PlaceOrderCommand(8L, 1)).order().getId();
        orderService.placeOrder(key, new PlaceOrderCommand(8L, 1));

        // 1건을 받은 뒤에도 잠시 더 기다려 두 번째가 오지 않는지 확인한다
        List<ConsumerRecord<String, String>> records = pollFor(orderId, Duration.ofSeconds(5), Integer.MAX_VALUE);

        assertThat(records).hasSize(1);
    }

    /** orderId가 일치하는 메시지만 모은다. expected건을 모으거나 timeout이 지나면 멈춘다. */
    private List<ConsumerRecord<String, String>> pollFor(Long orderId, Duration timeout, int expected) {
        List<ConsumerRecord<String, String>> matched = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline && matched.size() < expected) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
                if (jsonMapper.readTree(record.value()).get("orderId").asLong() == orderId) {
                    matched.add(record);
                }
            }
        }
        return matched;
    }
}
