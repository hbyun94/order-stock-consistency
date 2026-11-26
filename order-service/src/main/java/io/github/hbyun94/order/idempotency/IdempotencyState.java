package io.github.hbyun94.order.idempotency;

/** 멱등 키 선점 시도의 결과 */
public sealed interface IdempotencyState {

    /** 이 요청이 키를 처음 선점했다 → 주문을 처리한다 */
    record Acquired() implements IdempotencyState {
    }

    /** 같은 키의 다른 요청이 처리 중이다 → 409 */
    record InProgress(String fingerprint) implements IdempotencyState {
    }

    /** 같은 키의 요청이 이미 끝났다 → 저장된 주문 ID로 같은 응답을 다시 돌려준다 */
    record Completed(String fingerprint, long orderId) implements IdempotencyState {
    }
}
