package io.github.hbyun94.stock.domain;

/**
 * 예약 상태 전이 위반(이미 확정·해제된 예약을 다시 확정·해제). API에서는 409로 응답한다 (D-048).
 * 기존 코드와 테스트가 IllegalStateException으로 다루므로 그 하위 타입으로 둔다.
 */
public class ReservationStateException extends IllegalStateException {

    public ReservationStateException(String message) {
        super(message);
    }
}
