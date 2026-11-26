package io.github.hbyun94.order.web;

import io.github.hbyun94.order.domain.Order;

public record OrderResponse(Long orderId, Long productId, int quantity, String status) {

    static OrderResponse from(Order order) {
        return new OrderResponse(order.getId(), order.getProductId(), order.getQuantity(), order.getStatus().name());
    }
}
