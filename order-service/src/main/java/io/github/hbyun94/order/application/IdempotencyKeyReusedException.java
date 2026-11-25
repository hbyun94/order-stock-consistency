package io.github.hbyun94.order.application;

/** 같은 Idempotency-Key로 내용이 다른 요청이 왔다 (→ 422) */
public class IdempotencyKeyReusedException extends RuntimeException {

    public IdempotencyKeyReusedException(String idempotencyKey) {
        super("같은 Idempotency-Key로 다른 내용의 요청을 보낼 수 없습니다: " + idempotencyKey);
    }
}
