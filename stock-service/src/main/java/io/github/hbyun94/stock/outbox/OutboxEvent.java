package io.github.hbyun94.stock.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import java.time.Instant;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * 보낼 Kafka 메시지 한 건 (Transactional Outbox, D-036).
 * <p>
 * 업무 데이터와 같은 트랜잭션에서 저장되므로 "예약 결정은 커밋됐는데 결과 이벤트는 없는" 상태가 생기지 않는다.
 * 실제 전송은 {@link OutboxRelay}가 나중에 한다.
 */
@Entity
@Table(name = "outbox_event")
public class OutboxEvent {

    private static final int MAX_ERROR_LENGTH = 500;

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "outbox_event_seq")
    @SequenceGenerator(name = "outbox_event_seq", sequenceName = "outbox_event_seq", allocationSize = 50)
    private Long id;

    @Column(name = "topic", nullable = false, length = 100)
    private String topic;

    @Column(name = "message_key", nullable = false, length = 100)
    private String messageKey;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", nullable = false, columnDefinition = "jsonb")
    private String payload;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    @Column(name = "attempts", nullable = false)
    private int attempts;

    @Column(name = "last_error", length = MAX_ERROR_LENGTH)
    private String lastError;

    protected OutboxEvent() {
        // JPA 전용
    }

    private OutboxEvent(String topic, String messageKey, String payload) {
        this.topic = topic;
        this.messageKey = messageKey;
        this.payload = payload;
        this.createdAt = Instant.now();
    }

    public static OutboxEvent of(String topic, String messageKey, String payloadJson) {
        return new OutboxEvent(topic, messageKey, payloadJson);
    }

    void markPublished() {
        this.attempts++;
        this.publishedAt = Instant.now();
        this.lastError = null;
    }

    void markFailed(Throwable error) {
        this.attempts++;
        String message = error.getClass().getSimpleName() + ": " + error.getMessage();
        this.lastError = message.length() > MAX_ERROR_LENGTH ? message.substring(0, MAX_ERROR_LENGTH) : message;
    }

    public Long getId() {
        return id;
    }

    public String getTopic() {
        return topic;
    }

    public String getMessageKey() {
        return messageKey;
    }

    public String getPayload() {
        return payload;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }

    public int getAttempts() {
        return attempts;
    }

    public String getLastError() {
        return lastError;
    }
}
