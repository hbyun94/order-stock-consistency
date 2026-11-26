package io.github.hbyun94.order.web;

import io.github.hbyun94.order.application.OrderResult;
import io.github.hbyun94.order.application.OrderService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.net.URI;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

    static final String IDEMPOTENCY_KEY = "Idempotency-Key";
    /** 같은 키의 재요청에 저장된 결과를 돌려줬는지 알려준다 (Stripe와 같은 관례) */
    static final String IDEMPOTENT_REPLAYED = "Idempotent-Replayed";

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    /**
     * 주문 생성. 같은 Idempotency-Key의 재요청에는 처음과 같은 응답(201, 같은 주문)을 돌려준다.
     * 409: 같은 키의 요청이 처리 중 / 422: 같은 키로 다른 내용의 요청
     */
    @PostMapping
    public ResponseEntity<OrderResponse> placeOrder(
            @RequestHeader(IDEMPOTENCY_KEY) @NotBlank @Size(max = 100) String idempotencyKey,
            @RequestBody @Valid PlaceOrderRequest request) {
        OrderResult result = orderService.placeOrder(idempotencyKey, request.toCommand());
        return ResponseEntity.created(URI.create("/orders/" + result.order().getId()))
                .header(IDEMPOTENT_REPLAYED, String.valueOf(result.replayed()))
                .body(OrderResponse.from(result.order()));
    }

    @GetMapping("/{orderId}")
    public OrderResponse getOrder(@PathVariable Long orderId) {
        return OrderResponse.from(orderService.getOrder(orderId));
    }
}
