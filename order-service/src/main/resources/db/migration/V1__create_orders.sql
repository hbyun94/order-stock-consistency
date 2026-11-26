-- 주문. 테이블명은 SQL 예약어 ORDER를 피해 orders로 한다.
CREATE SEQUENCE orders_seq START WITH 1 INCREMENT BY 50;

CREATE TABLE orders (
    id                  BIGINT       PRIMARY KEY,
    -- 클라이언트가 보낸 Idempotency-Key. Redis가 키를 잃어도 이 제약이 중복 주문을 막는 최종 방어선이다 (D-019)
    idempotency_key     VARCHAR(100) NOT NULL,
    -- 요청 내용(상품, 수량)의 SHA-256. 같은 키로 다른 내용이 오면 거절하기 위해 저장한다 (D-020)
    request_fingerprint VARCHAR(64)  NOT NULL,
    product_id          BIGINT       NOT NULL,
    quantity            INT          NOT NULL,
    status              VARCHAR(20)  NOT NULL,
    created_at          TIMESTAMPTZ  NOT NULL,
    updated_at          TIMESTAMPTZ  NOT NULL,
    CONSTRAINT uk_orders_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT ck_orders_quantity_positive CHECK (quantity > 0),
    CONSTRAINT ck_orders_status CHECK (status IN ('PENDING', 'RESERVED', 'CANCELLED'))
);
