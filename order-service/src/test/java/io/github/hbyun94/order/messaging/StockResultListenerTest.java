package io.github.hbyun94.order.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.hbyun94.order.TestcontainersConfiguration;
import io.github.hbyun94.order.application.OrderService;
import io.github.hbyun94.order.application.PlaceOrderCommand;
import io.github.hbyun94.order.domain.OrderRepository;
import io.github.hbyun94.order.domain.OrderStatus;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * 실제 Kafka에서 재고 예약 결과 이벤트 → 주문 상태 변경을 검증한다.
 * stock-service 대신 테스트가 결과 이벤트를 직접 보낸다 (JSON 필드 계약만 맞으면 된다, D-028).
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class StockResultListenerTest {

    @Autowired
    OrderService orderService;

    @Autowired
    OrderRepository orderRepository;

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @Test
    @DisplayName("stock.reserved를 받으면 주문이 RESERVED가 된다")
    void reserved() {
        Long orderId = placePendingOrder();

        kafkaTemplate.send(KafkaTopics.STOCK_RESERVED, String.valueOf(orderId),
                new StockReservedEvent(orderId, 1L, 2)).join();

        awaitStatus(orderId, OrderStatus.RESERVED);
    }

    @Test
    @DisplayName("stock.reservation-failed를 받으면 주문이 CANCELLED가 된다 (보상 처리)")
    void cancelled() {
        Long orderId = placePendingOrder();

        kafkaTemplate.send(KafkaTopics.STOCK_RESERVATION_FAILED, String.valueOf(orderId),
                new StockReservationFailedEvent(orderId, 1L, 2, "INSUFFICIENT_STOCK")).join();

        awaitStatus(orderId, OrderStatus.CANCELLED);
    }

    private Long placePendingOrder() {
        Long orderId = orderService.placeOrder(UUID.randomUUID().toString(), new PlaceOrderCommand(1L, 2))
                .order().getId();
        assertThat(orderRepository.findById(orderId).orElseThrow().getStatus()).isEqualTo(OrderStatus.PENDING);
        return orderId;
    }

    private void awaitStatus(Long orderId, OrderStatus expected) {
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(orderRepository.findById(orderId).orElseThrow().getStatus()).isEqualTo(expected));
    }
}
