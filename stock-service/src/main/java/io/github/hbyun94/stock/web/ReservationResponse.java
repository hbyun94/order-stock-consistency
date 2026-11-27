package io.github.hbyun94.stock.web;

import io.github.hbyun94.stock.domain.ReservationStatus;
import io.github.hbyun94.stock.domain.StockReservation;

public record ReservationResponse(Long orderId, Long productId, int quantity, ReservationStatus status) {

    static ReservationResponse from(StockReservation reservation) {
        return new ReservationResponse(reservation.getOrderId(), reservation.getProductId(),
                reservation.getQuantity(), reservation.getStatus());
    }
}
