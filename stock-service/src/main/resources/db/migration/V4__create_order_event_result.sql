-- order.created 처리 결과. 주문 하나에 대한 예약 결정(성공/실패)을 딱 한 번만 기록한다 (D-033).
-- 같은 이벤트가 다시 오면 새로 판단하지 않고 여기 기록된 결과를 그대로 다시 보낸다.
-- 미등록 상품의 실패도 기록해야 하므로 stock에 대한 FK는 두지 않는다.
CREATE TABLE order_event_result (
    order_id   BIGINT      PRIMARY KEY,
    product_id BIGINT      NOT NULL,
    quantity   INT         NOT NULL,
    outcome    VARCHAR(20) NOT NULL,
    reason     VARCHAR(30),
    created_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT ck_order_event_result_outcome CHECK (outcome IN ('RESERVED', 'REJECTED')),
    -- 실패일 때만 사유가 있다
    CONSTRAINT ck_order_event_result_reason CHECK ((outcome = 'REJECTED') = (reason IS NOT NULL))
);
