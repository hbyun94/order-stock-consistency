package io.github.hbyun94.stock.domain;

/** 재고 예약 거절 사유. 결과 이벤트의 reason 필드에 이름(문자열)으로 실린다. */
public enum RejectionReason {
    /** 가용 재고 부족 */
    INSUFFICIENT_STOCK,
    /** 재고가 등록되지 않은 상품 */
    UNKNOWN_PRODUCT
}
