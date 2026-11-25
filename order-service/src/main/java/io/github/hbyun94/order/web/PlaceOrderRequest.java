package io.github.hbyun94.order.web;

import io.github.hbyun94.order.application.PlaceOrderCommand;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

public record PlaceOrderRequest(
        @NotNull @Positive Long productId,
        @NotNull @Positive Integer quantity) {

    PlaceOrderCommand toCommand() {
        return new PlaceOrderCommand(productId, quantity);
    }
}
