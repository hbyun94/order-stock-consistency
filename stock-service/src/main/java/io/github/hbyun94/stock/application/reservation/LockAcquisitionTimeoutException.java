package io.github.hbyun94.stock.application.reservation;

/** 분산 락을 대기 시간 안에 얻지 못했을 때 발생한다. */
public class LockAcquisitionTimeoutException extends RuntimeException {

    public LockAcquisitionTimeoutException(String lockKey, long waitMillis) {
        super("분산 락 획득 실패: key=" + lockKey + ", 대기=" + waitMillis + "ms");
    }
}
