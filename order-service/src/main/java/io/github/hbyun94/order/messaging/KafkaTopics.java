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

    /** stock.reserved의 DLT. order-service가 소비하다 실패한 메시지가 간다 */
    public static final String STOCK_RESERVED_DLT = STOCK_RESERVED + ".dlt";

    /** stock.reservation-failed의 DLT */
    public static final String STOCK_RESERVATION_FAILED_DLT = STOCK_RESERVATION_FAILED + ".dlt";

    /** 처리하지 못한 메시지를 보내는 토픽 이름: 원래 토픽 + ".dlt" (D-045) */
    public static String deadLetterOf(String topic) {
        return topic + ".dlt";
    }

    private KafkaTopics() {
    }
}
