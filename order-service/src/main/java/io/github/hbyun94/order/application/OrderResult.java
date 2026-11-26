package io.github.hbyun94.order.application;

import io.github.hbyun94.order.domain.Order;

/**
 * 주문 요청 결과.
 *
 * @param replayed true면 새로 만든 주문이 아니라, 같은 Idempotency-Key로 이미 만들어진 주문을 다시 돌려준 것
 */
public record OrderResult(Order order, boolean replayed) {

    static OrderResult created(Order order) {
        return new OrderResult(order, false);
    }

    static OrderResult replayed(Order order) {
        return new OrderResult(order, true);
    }
}
