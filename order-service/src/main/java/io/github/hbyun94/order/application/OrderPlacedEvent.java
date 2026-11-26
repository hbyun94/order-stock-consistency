package io.github.hbyun94.order.application;

/**
 * 주문이 저장됐다는 애플리케이션 내부 이벤트 (Spring ApplicationEvent).
 * 트랜잭션 안에서 발행하고, 커밋된 뒤에 Kafka로 내보낸다 (OrderCreatedPublisher).
 */
public record OrderPlacedEvent(Long orderId, Long productId, int quantity) {
}
