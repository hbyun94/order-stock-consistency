package io.github.hbyun94.order.messaging;

/** stock.reserved 메시지 (stock-service가 발행) */
public record StockReservedEvent(Long orderId, Long productId, int quantity) {
}
