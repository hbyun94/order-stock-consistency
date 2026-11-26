package io.github.hbyun94.order.messaging;

/**
 * stock.reservation-failed 메시지 (stock-service가 발행).
 *
 * @param reason INSUFFICIENT_STOCK(재고 부족) 또는 UNKNOWN_PRODUCT(재고 미등록 상품)
 */
public record StockReservationFailedEvent(Long orderId, Long productId, int quantity, String reason) {
}
