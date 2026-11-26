package io.github.hbyun94.stock.domain;

import jakarta.persistence.LockModeType;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface StockRepository extends JpaRepository<Stock, Long> {

    Optional<Stock> findByProductId(Long productId);

    /**
     * 비관적 쓰기 락으로 조회한다. PostgreSQL에서는 {@code SELECT ... FOR NO KEY UPDATE}로 실행된다.
     * 이 트랜잭션이 끝날 때까지 다른 트랜잭션의 같은 조회는 대기한다.
     * 타임아웃은 두지 않는다 (D-017: 실험에서 순수 대기 비용을 보기 위함).
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select s from Stock s where s.productId = :productId")
    Optional<Stock> findByProductIdForUpdate(@Param("productId") Long productId);
}
