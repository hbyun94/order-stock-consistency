-- 주문별 재고 예약 이력. 주문 한 건 = 상품 한 종류 (D-007)
-- order_id UNIQUE: 한 주문은 한 번만 예약된다 (6단계 중복 소비 방지의 기반)
CREATE SEQUENCE stock_reservation_seq START WITH 1 INCREMENT BY 50;

CREATE TABLE stock_reservation (
    id         BIGINT      PRIMARY KEY,
    order_id   BIGINT      NOT NULL,
    product_id BIGINT      NOT NULL,
    quantity   INT         NOT NULL,
    status     VARCHAR(20) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT uk_stock_reservation_order_id UNIQUE (order_id),
    CONSTRAINT fk_stock_reservation_product FOREIGN KEY (product_id) REFERENCES stock (product_id),
    CONSTRAINT ck_stock_reservation_quantity_positive CHECK (quantity > 0),
    CONSTRAINT ck_stock_reservation_status CHECK (status IN ('RESERVED', 'CONFIRMED', 'RELEASED'))
);
