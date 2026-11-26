package io.github.hbyun94.order.domain;

/**
 * 주문 상태.
 * <pre>
 * PENDING ──재고 예약 성공──▶ RESERVED
 *    └────재고 부족───────▶ CANCELLED (보상 처리)
 * </pre>
 * 전이는 stock-service의 재고 예약 결과 이벤트로 일어난다 (5단계).
 */
public enum OrderStatus {
    PENDING,
    RESERVED,
    CANCELLED
}
