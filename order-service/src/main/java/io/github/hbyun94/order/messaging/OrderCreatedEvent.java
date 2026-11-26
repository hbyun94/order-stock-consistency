package io.github.hbyun94.order.messaging;

import java.time.Instant;

/** order.created 메시지. stock-service의 같은 이름 record와 JSON 필드로 맞춘다 (D-028). */
public record OrderCreatedEvent(Long orderId, Long productId, int quantity, Instant occurredAt) {
}
