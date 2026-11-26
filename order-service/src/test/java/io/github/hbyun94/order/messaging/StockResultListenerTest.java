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

    @Test
    @DisplayName("[중복 소비] stock.reserved가 두 번 와도 주문은 RESERVED 그대로이고 예외 없이 처리된다")
    void duplicateReserved() {
        Long orderId = placePendingOrder();

        orderService.markReserved(orderId);
        orderService.markReserved(orderId); // 예외가 나지 않아야 한다

        assertThat(orderRepository.findById(orderId).orElseThrow().getStatus()).isEqualTo(OrderStatus.RESERVED);
    }

    @Test
    @DisplayName("[모순] 이미 RESERVED인 주문에 실패 이벤트가 와도 예외 없이 무시하고 상태를 바꾸지 않는다")
    void conflictingResultIgnored() {
        Long orderId = placePendingOrder();
        orderService.markReserved(orderId);

        orderService.cancel(orderId); // 예외가 나지 않아야 한다

        assertThat(orderRepository.findById(orderId).orElseThrow().getStatus()).isEqualTo(OrderStatus.RESERVED);
    }

    @Test
    @DisplayName("[중복 소비] Kafka로 같은 실패 이벤트가 두 번 와도 주문은 CANCELLED 한 번으로 끝나고 다음 메시지도 처리된다")
    void duplicateFailedThroughKafka() {
        Long orderId = placePendingOrder();
        Long nextOrderId = placePendingOrder();
        StockReservationFailedEvent failed = new StockReservationFailedEvent(orderId, 1L, 2, "INSUFFICIENT_STOCK");

        kafkaTemplate.send(KafkaTopics.STOCK_RESERVATION_FAILED, String.valueOf(orderId), failed).join();
        kafkaTemplate.send(KafkaTopics.STOCK_RESERVATION_FAILED, String.valueOf(orderId), failed).join();
        // 이어서 다른 주문의 결과를 보낸다: 중복 처리 후에도 소비가 계속되는지 확인
        kafkaTemplate.send(KafkaTopics.STOCK_RESERVED, String.valueOf(nextOrderId),
                new StockReservedEvent(nextOrderId, 1L, 2)).join();

        awaitStatus(orderId, OrderStatus.CANCELLED);
        awaitStatus(nextOrderId, OrderStatus.RESERVED);
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
