package io.github.hbyun94.stock.messaging;

/** 재고 예약 실패 이유. JSON에는 이름(문자열)으로 실린다. */
public enum ReservationFailureReason {
    /** 가용 재고 부족 */
    INSUFFICIENT_STOCK,
    /** 재고가 등록되지 않은 상품 */
    UNKNOWN_PRODUCT
}
