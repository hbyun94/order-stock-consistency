package io.github.hbyun94.stock.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import java.time.Instant;
import org.hibernate.annotations.CreationTimestamp;
import org.springframework.data.domain.Persistable;

/**
 * 주문 하나(order.created)에 대한 처리 결과 기록. 중복 소비를 막는 기준이다 (D-033).
 * <p>
 * PK가 주문 ID라서, 같은 주문을 두 번 기록하려 하면 DB가 PK 위반으로 막는다.
 * <p>
 * <b>Persistable을 구현하는 이유:</b> ID를 직접 넣는 엔티티를 Spring Data {@code save()}에 넘기면
 * "ID가 있으니 기존 엔티티"로 보고 {@code merge}를 한다. merge는 같은 ID의 행이 이미 있으면 그 행을 덮어쓴다(UPDATE).
 * 중복을 막아야 할 자리에서 조용히 덮어쓰면 안 되므로, 새로 만든 객체는 isNew()=true로 알려 항상 INSERT(persist)하게 한다.
 */
@Entity
@Table(name = "order_event_result")
public class OrderEventResult implements Persistable<Long> {

    @Id
    @Column(name = "order_id")
    private Long orderId;

    @Column(name = "product_id", nullable = false)
    private Long productId;

    @Column(name = "quantity", nullable = false)
    private int quantity;

    @Enumerated(EnumType.STRING)
    @Column(name = "outcome", nullable = false, length = 20)
    private ReservationOutcome outcome;

    @Enumerated(EnumType.STRING)
    @Column(name = "reason", length = 30)
    private RejectionReason reason;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Transient
    private boolean isNew = false;

    protected OrderEventResult() {
        // JPA 전용 (DB에서 읽은 객체는 isNew=false)
    }

    private OrderEventResult(Long orderId, Long productId, int quantity, ReservationOutcome outcome, RejectionReason reason) {
        this.orderId = orderId;
        this.productId = productId;
        this.quantity = quantity;
        this.outcome = outcome;
        this.reason = reason;
        this.isNew = true;
    }

    public static OrderEventResult reserved(Long orderId, Long productId, int quantity) {
        return new OrderEventResult(orderId, productId, quantity, ReservationOutcome.RESERVED, null);
    }

    public static OrderEventResult rejected(Long orderId, Long productId, int quantity, RejectionReason reason) {
        if (reason == null) {
            throw new IllegalArgumentException("거절 사유는 필수입니다");
        }
        return new OrderEventResult(orderId, productId, quantity, ReservationOutcome.REJECTED, reason);
    }

    @Override
    public Long getId() {
        return orderId;
    }

    @Override
    public boolean isNew() {
        return isNew;
    }

    public Long getOrderId() {
        return orderId;
    }

    public Long getProductId() {
        return productId;
    }

    public int getQuantity() {
        return quantity;
    }

    public ReservationOutcome getOutcome() {
        return outcome;
    }

    public RejectionReason getReason() {
        return reason;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
