-- 상품별 재고. 가용 재고 = on_hand - reserved
-- on_hand : 창고에 실제로 있는 수량 (출고 확정 시 감소)
-- reserved: 주문으로 예약됐지만 아직 출고되지 않은 수량
CREATE SEQUENCE stock_seq START WITH 1 INCREMENT BY 50;

CREATE TABLE stock (
    id         BIGINT PRIMARY KEY,
    product_id BIGINT NOT NULL,
    on_hand    INT    NOT NULL,
    reserved   INT    NOT NULL,
    CONSTRAINT uk_stock_product_id UNIQUE (product_id),
    CONSTRAINT ck_stock_on_hand_non_negative CHECK (on_hand >= 0),
    CONSTRAINT ck_stock_reserved_non_negative CHECK (reserved >= 0),
    -- DB 차원의 마지막 방어선. 단, 동시 갱신에서 생기는 lost update는 이 제약으로 막을 수 없다.
    CONSTRAINT ck_stock_reserved_le_on_hand CHECK (reserved <= on_hand)
);
