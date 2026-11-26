package io.github.hbyun94.stock.messaging;

import io.github.hbyun94.stock.domain.RejectionReason;

/** stock.reservation-failed 메시지 (stock-service가 발행). reason은 이름(문자열)으로 실린다. */
public record StockReservationFailedEvent(Long orderId, Long productId, int quantity, RejectionReason reason) {
}
