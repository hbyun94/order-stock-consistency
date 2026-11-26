package io.github.hbyun94.stock.application;

import io.github.hbyun94.stock.domain.OrderEventResult;

/**
 * 재고 예약 결정(성공/거절)을 결과 이벤트로 내보내는 출구. 구현은 messaging 계층(Outbox)이 맡는다.
 * <p>
 * <b>반드시 결정을 기록하는 트랜잭션 안에서 호출한다.</b> 결정과 결과 이벤트가 함께 커밋되거나 함께 롤백된다 (D-036).
 */
public interface ReservationResultEvents {

    void resultDecided(OrderEventResult result);
}
