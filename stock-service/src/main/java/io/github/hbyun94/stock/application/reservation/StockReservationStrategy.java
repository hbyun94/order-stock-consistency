package io.github.hbyun94.stock.application.reservation;

import io.github.hbyun94.stock.domain.StockReservation;

/**
 * 재고 예약을 동시성 제어 방식별로 바꿔 끼우기 위한 전략 (실험 1, D-012).
 * <p>
 * 구현체 4개: 락 없음 / 낙관적 락 / 비관적 락 / Redis 분산 락.
 * 모두 같은 계약을 지킨다: 성공하면 예약 기록을 반환하고, 가용 재고가 부족하면
 * {@link io.github.hbyun94.stock.domain.InsufficientStockException}을 던지며 아무것도 저장하지 않는다.
 */
public interface StockReservationStrategy {

    /** 실험 결과 표에 쓸 이름 */
    String name();

    StockReservation reserve(Long orderId, Long productId, int quantity);
}
