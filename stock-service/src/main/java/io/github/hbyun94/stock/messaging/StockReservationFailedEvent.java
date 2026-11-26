package io.github.hbyun94.stock.messaging;

/** stock.reservation-failed 메시지 (stock-service가 발행) */
public record StockReservationFailedEvent(Long orderId, Long productId, int quantity, ReservationFailureReason reason) {
}
