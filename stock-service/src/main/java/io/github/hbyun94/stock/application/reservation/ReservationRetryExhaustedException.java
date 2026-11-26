package io.github.hbyun94.stock.application.reservation;

/** 낙관적 락 충돌이 최대 시도 횟수만큼 반복돼 예약을 포기했을 때 발생한다. */
public class ReservationRetryExhaustedException extends RuntimeException {

    public ReservationRetryExhaustedException(Long orderId, int attempts, Throwable lastCause) {
        super("낙관적 락 충돌로 " + attempts + "회 시도 후 예약 실패: orderId=" + orderId, lastCause);
    }
}
