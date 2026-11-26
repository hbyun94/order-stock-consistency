package io.github.hbyun94.stock.application;

public class ReservationNotFoundException extends RuntimeException {

    public ReservationNotFoundException(Long orderId) {
        super("예약이 없는 주문입니다: orderId=" + orderId);
    }
}
