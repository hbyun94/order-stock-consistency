package io.github.hbyun94.order.messaging;

/**
 * 이벤트 종류별 토픽 (D-026).
 * 같은 이름을 stock-service도 정의한다. 서비스 간 공유 코드는 두지 않고 이름과 JSON 필드로만 맞춘다 (D-028).
 */
public final class KafkaTopics {

    /** 주문 생성 (order-service 발행). 키: 상품 ID */
    public static final String ORDER_CREATED = "order.created";

    /** 재고 예약 성공 (stock-service 발행). 키: 주문 ID */
    public static final String STOCK_RESERVED = "stock.reserved";

    /** 재고 예약 실패 (stock-service 발행). 키: 주문 ID */
    public static final String STOCK_RESERVATION_FAILED = "stock.reservation-failed";

    private KafkaTopics() {
    }
}
