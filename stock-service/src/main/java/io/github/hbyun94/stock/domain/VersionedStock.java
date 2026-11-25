package io.github.hbyun94.stock.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

/**
 * 낙관적 락용 재고 엔티티. {@link Stock}과 같은 stock 테이블을 매핑한다 (D-012).
 * <p>
 * {@code @Version}이 있으면 Hibernate는 UPDATE를 다음처럼 만든다.
 * <pre>
 * UPDATE stock SET reserved = ?, version = version + 1 WHERE id = ? AND version = ?
 * </pre>
 * 읽은 뒤 다른 트랜잭션이 먼저 고쳤다면 version이 달라져 0건이 갱신되고,
 * Hibernate가 이를 감지해 낙관적 락 예외를 던진다. 즉 lost update가 "덮어쓰기" 대신 "실패"가 된다.
 * <p>
 * 재고 생성은 {@link Stock#create}로 하고, 이 엔티티는 조회와 갱신에만 쓴다.
 */
@Entity
@Table(name = "stock")
public class VersionedStock extends AbstractStock {

    @Version
    @Column(name = "version", nullable = false)
    private Long version;

    protected VersionedStock() {
        // JPA 전용
    }

    public Long getVersion() {
        return version;
    }
}
