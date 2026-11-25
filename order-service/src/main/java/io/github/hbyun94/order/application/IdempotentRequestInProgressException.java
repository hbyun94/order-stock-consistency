package io.github.hbyun94.order.application;

/** 같은 Idempotency-Key의 요청이 아직 처리 중이다 (→ 409, 잠시 후 재시도) */
public class IdempotentRequestInProgressException extends RuntimeException {

    public IdempotentRequestInProgressException(String idempotencyKey) {
        super("같은 Idempotency-Key의 요청이 처리 중입니다: " + idempotencyKey);
    }
}
