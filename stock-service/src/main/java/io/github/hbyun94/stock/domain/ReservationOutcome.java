package io.github.hbyun94.stock.domain;

/** order.created 처리 결과 */
public enum ReservationOutcome {
    /** 재고 예약 성공 */
    RESERVED,
    /** 예약 거절 (재고 부족, 미등록 상품) */
    REJECTED
}
