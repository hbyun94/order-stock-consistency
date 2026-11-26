package io.github.hbyun94.order.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import java.time.Instant;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

/** 주문 한 건 = 상품 한 종류 (D-007) */
@Entity
@Table(name = "orders")
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "orders_seq")
    @SequenceGenerator(name = "orders_seq", sequenceName = "orders_seq", allocationSize = 50)
    private Long id;

    @Column(name = "idempotency_key", nullable = false, unique = true, length = 100)
    private String idempotencyKey;

    @Column(name = "request_fingerprint", nullable = false, length = 64)
    private String requestFingerprint;

    @Column(name = "product_id", nullable = false)
    private Long productId;

    @Column(name = "quantity", nullable = false)
    private int quantity;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private OrderStatus status;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected Order() {
        // JPA 전용
    }

    private Order(String idempotencyKey, String requestFingerprint, Long productId, int quantity) {
        this.idempotencyKey = idempotencyKey;
        this.requestFingerprint = requestFingerprint;
        this.productId = productId;
        this.quantity = quantity;
        this.status = OrderStatus.PENDING;
    }

    /** 새 주문은 재고 예약 결과를 기다리는 PENDING 상태로 시작한다. */
    public static Order place(String idempotencyKey, String requestFingerprint, Long productId, int quantity) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new IllegalArgumentException("idempotencyKey는 필수입니다");
        }
        if (requestFingerprint == null || requestFingerprint.length() != 64) {
            throw new IllegalArgumentException("requestFingerprint는 SHA-256 16진수 64자여야 합니다");
        }
        if (productId == null) {
            throw new IllegalArgumentException("productId는 필수입니다");
        }
        if (quantity <= 0) {
            throw new IllegalArgumentException("수량은 1 이상이어야 합니다: " + quantity);
        }
        return new Order(idempotencyKey, requestFingerprint, productId, quantity);
    }

    /**
     * 재고 예약 성공: PENDING → RESERVED.
     * 이미 RESERVED면 같은 결과 이벤트가 다시 온 것(중복 소비)이므로 아무것도 하지 않는다 (D-034).
     *
     * @return 상태가 바뀌었으면 true, 이미 RESERVED라서 그대로면 false
     * @throws IllegalStateException CANCELLED처럼 모순된 상태일 때
     */
    public boolean markReserved() {
        if (status == OrderStatus.RESERVED) {
            return false;
        }
        requirePending("재고 예약 완료");
        this.status = OrderStatus.RESERVED;
        return true;
    }

    /**
     * 재고 예약 실패에 따른 보상 처리: PENDING → CANCELLED.
     * 이미 CANCELLED면 중복 소비이므로 아무것도 하지 않는다 (D-034).
     *
     * @return 상태가 바뀌었으면 true, 이미 CANCELLED라서 그대로면 false
     * @throws IllegalStateException RESERVED처럼 모순된 상태일 때
     */
    public boolean cancel() {
        if (status == OrderStatus.CANCELLED) {
            return false;
        }
        requirePending("취소");
        this.status = OrderStatus.CANCELLED;
        return true;
    }

    private void requirePending(String action) {
        if (status != OrderStatus.PENDING) {
            throw new IllegalStateException(
                    "PENDING 상태에서만 " + action + " 처리할 수 있습니다: orderId=" + id + ", 현재 상태=" + status);
        }
    }

    public Long getId() {
        return id;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public String getRequestFingerprint() {
        return requestFingerprint;
    }

    public Long getProductId() {
        return productId;
    }

    public int getQuantity() {
        return quantity;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
