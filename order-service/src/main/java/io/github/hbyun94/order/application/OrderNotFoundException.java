package io.github.hbyun94.order.application;

public class OrderNotFoundException extends RuntimeException {

    public OrderNotFoundException(Long orderId) {
        super("주문이 없습니다: orderId=" + orderId);
    }
}
