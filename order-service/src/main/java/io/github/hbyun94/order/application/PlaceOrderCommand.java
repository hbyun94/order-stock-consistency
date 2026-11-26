package io.github.hbyun94.order.application;

/** 주문 요청 내용. 요청 지문(fingerprint)은 이 값으로 계산한다. */
public record PlaceOrderCommand(Long productId, int quantity) {
}
