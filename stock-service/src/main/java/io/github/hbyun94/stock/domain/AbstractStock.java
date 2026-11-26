package io.github.hbyun94.stock.domain;

import jakarta.persistence.Column;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.SequenceGenerator;

/**
 * 재고의 필드와 업무 규칙을 담은 공통 부모 클래스.
 * <p>
 * 예약과 출고 확정을 분리한다.
 * <ul>
 *   <li>예약(reserve): reserved 증가 → 가용 재고 감소, 실물(onHand)은 그대로</li>
 *   <li>확정(confirm): 예약분을 실제 출고 → onHand와 reserved를 함께 감소</li>
 *   <li>복원(release): 예약 취소 → reserved 감소, 가용 재고 복원</li>
 * </ul>
 * 같은 stock 테이블을 두 엔티티가 매핑한다 (D-012).
 * <ul>
 *   <li>{@link Stock}: version 없음 → 락 없음, 비관적 락, 분산 락에서 사용</li>
 *   <li>{@link VersionedStock}: @Version 있음 → 낙관적 락에서 사용</li>
 * </ul>
 * 규칙을 여기 한 곳에 두어 두 엔티티가 똑같은 규칙으로 동작하게 한다.
 */
@MappedSuperclass
public abstract class AbstractStock {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "stock_seq")
    @SequenceGenerator(name = "stock_seq", sequenceName = "stock_seq", allocationSize = 50)
    private Long id;

    @Column(name = "product_id", nullable = false, unique = true)
    private Long productId;

    @Column(name = "on_hand", nullable = false)
    private int onHand;

    @Column(name = "reserved", nullable = false)
    private int reserved;

    protected AbstractStock() {
        // JPA 전용
    }

    protected AbstractStock(Long productId, int onHand) {
        if (productId == null) {
            throw new IllegalArgumentException("productId는 필수입니다");
        }
        if (onHand < 0) {
            throw new IllegalArgumentException("초기 재고는 0 이상이어야 합니다: " + onHand);
        }
        this.productId = productId;
        this.onHand = onHand;
        this.reserved = 0;
    }

    /** 가용 재고: 새 주문이 예약할 수 있는 수량 */
    public int available() {
        return onHand - reserved;
    }

    /** 재고 예약. 가용 재고가 부족하면 상태를 바꾸지 않고 예외를 던진다. */
    public void reserve(int quantity) {
        requirePositive(quantity);
        if (available() < quantity) {
            throw new InsufficientStockException(productId, quantity, available());
        }
        reserved += quantity;
    }

    /** 출고 확정. 예약돼 있던 수량만큼 실물 재고를 차감한다. */
    public void confirm(int quantity) {
        requirePositive(quantity);
        requireReservedAtLeast(quantity);
        onHand -= quantity;
        reserved -= quantity;
    }

    /** 예약 복원. 주문 취소 등으로 예약을 풀어 가용 재고를 되돌린다. */
    public void release(int quantity) {
        requirePositive(quantity);
        requireReservedAtLeast(quantity);
        reserved -= quantity;
    }

    private static void requirePositive(int quantity) {
        if (quantity <= 0) {
            throw new IllegalArgumentException("수량은 1 이상이어야 합니다: " + quantity);
        }
    }

    private void requireReservedAtLeast(int quantity) {
        if (reserved < quantity) {
            throw new IllegalStateException(
                    "예약 수량(" + reserved + ")보다 많이 확정/복원할 수 없습니다: " + quantity);
        }
    }

    public Long getId() {
        return id;
    }

    public Long getProductId() {
        return productId;
    }

    public int getOnHand() {
        return onHand;
    }

    public int getReserved() {
        return reserved;
    }
}
