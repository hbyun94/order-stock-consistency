-- Transactional Outbox (D-036). 업무 데이터와 같은 트랜잭션으로 "보낼 메시지"를 저장한다.
-- 발행기(OutboxRelay)가 미발행 행을 읽어 Kafka로 보내고 published_at을 기록한다.
CREATE SEQUENCE outbox_event_seq START WITH 1 INCREMENT BY 50;

CREATE TABLE outbox_event (
    id           BIGINT       PRIMARY KEY,
    topic        VARCHAR(100) NOT NULL,
    message_key  VARCHAR(100) NOT NULL,
    -- Kafka로 그대로 보낼 메시지 본문(JSON). 저장 시점에 직렬화해 두므로 발행기는 형식을 몰라도 된다
    payload      JSONB        NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL,
    published_at TIMESTAMPTZ,
    attempts     INT          NOT NULL DEFAULT 0,
    last_error   VARCHAR(500)
);

-- 발행기는 "아직 안 보낸 행"만 찾는다. 보낸 행이 쌓여도 이 부분 인덱스는 미발행 행만 담아 작게 유지된다
CREATE INDEX ix_outbox_event_unpublished ON outbox_event (id) WHERE published_at IS NULL;
