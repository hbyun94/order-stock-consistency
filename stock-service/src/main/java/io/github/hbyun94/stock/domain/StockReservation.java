package io.github.hbyun94.stock.domain;

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

/**
 * 주문 한 건의 재고 예약 기록.
 * <p>
 * Stock과 연관관계(@ManyToOne)를 맺지 않고 productId 값만 가진다.
 * 재고(Stock)와 예약(StockReservation)은 수명과 변경 이유가 다른 별도 단위이고,
 * 둘을 함께 바꾸는 일은 서비스 계층의 트랜잭션이 맡는다.
 */
@Entity
@Table(name = "stock_reservation")
public class StockReservation {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "stock_reservation_seq")
    @SequenceGenerator(name = "stock_reservation_seq", sequenceName = "stock_reservation_seq", allocationSize = 50)
    private Long id;

    @Column(name = "order_id", nullable = false, unique = true)
    private Long orderId;

    @Column(name = "product_id", nullable = false)
    private Long productId;

    @Column(name = "quantity", nullable = false)
    private int quantity;

    // 기본값(ORDINAL)은 enum 순서가 바뀌면 기존 데이터의 의미가 바뀌므로 이름으로 저장한다
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private ReservationStatus status;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected StockReservation() {
        // JPA 전용
    }

    private StockReservation(Long orderId, Long productId, int quantity) {
        this.orderId = orderId;
        this.productId = productId;
        this.quantity = quantity;
        this.status = ReservationStatus.RESERVED;
    }

    public static StockReservation reserve(Long orderId, Long productId, int quantity) {
        if (orderId == null || productId == null) {
            throw new IllegalArgumentException("orderId와 productId는 필수입니다");
        }
        if (quantity <= 0) {
            throw new IllegalArgumentException("수량은 1 이상이어야 합니다: " + quantity);
        }
        return new StockReservation(orderId, productId, quantity);
    }

    /** 출고 확정. RESERVED 상태에서만 가능하다. */
    public void confirm() {
        requireReserved("확정");
        this.status = ReservationStatus.CONFIRMED;
    }

    /** 예약 복원. RESERVED 상태에서만 가능하다 (이미 출고된 예약은 되돌리지 않는다). */
    public void release() {
        requireReserved("복원");
        this.status = ReservationStatus.RELEASED;
    }

    private void requireReserved(String action) {
        if (status != ReservationStatus.RESERVED) {
            throw new IllegalStateException(
                    "RESERVED 상태에서만 " + action + "할 수 있습니다: orderId=" + orderId + ", 현재 상태=" + status);
        }
    }

    public Long getId() {
        return id;
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

    public ReservationStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
