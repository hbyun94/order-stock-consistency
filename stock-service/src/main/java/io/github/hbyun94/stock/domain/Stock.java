package io.github.hbyun94.stock.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.Table;

/**
 * 상품 하나의 재고 (version 없음).
 * <p>
 * 이 엔티티 자체에는 동시성 제어가 없다. 락 없음 / 비관적 락 / 분산 락 전략이 사용한다.
 * 필드와 규칙은 {@link AbstractStock}에 있다.
 */
@Entity
@Table(name = "stock")
public class Stock extends AbstractStock {

    protected Stock() {
        // JPA 전용
    }

    private Stock(Long productId, int onHand) {
        super(productId, onHand);
    }

    public static Stock create(Long productId, int initialQuantity) {
        return new Stock(productId, initialQuantity);
    }
}
