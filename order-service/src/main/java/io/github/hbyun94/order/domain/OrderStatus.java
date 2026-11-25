package io.github.hbyun94.order.domain;

/**
 * 주문 상태.
 * <pre>
 * PENDING ──재고 예약 성공──▶ RESERVED
 *    └────재고 부족───────▶ CANCELLED (보상 처리)
 * </pre>
 * 4단계에서는 PENDING으로 저장만 한다. 상태 전이는 5단계에서 재고 이벤트와 함께 만든다.
 */
public enum OrderStatus {
    PENDING,
    RESERVED,
    CANCELLED
}
