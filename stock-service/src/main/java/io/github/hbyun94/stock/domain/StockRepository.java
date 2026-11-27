package io.github.hbyun94.stock.domain;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;

public interface StockRepository extends JpaRepository<Stock, Long> {

    Optional<Stock> findByProductId(Long productId);

    /**
     * 비관적 쓰기 락으로 조회한다. PostgreSQL에서는 {@code SELECT ... FOR NO KEY UPDATE}로 실행된다.
     * 이 트랜잭션이 끝날 때까지 다른 트랜잭션의 같은 조회는 대기한다.
     * 타임아웃은 두지 않는다 (D-017: 실험에서 순수 대기 비용을 보기 위함). 실제 예약 흐름은 {@link #findByProductIdForReservation}을 쓴다.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select s from Stock s where s.productId = :productId")
    Optional<Stock> findByProductIdForUpdate(@Param("productId") Long productId);

    /** 예약 흐름의 락 대기 상한(밀리초). D-047 */
    String RESERVATION_LOCK_TIMEOUT_MS = "3000";

    /**
     * 실제 예약 흐름(order.created 처리)에서 쓰는 비관적 락 조회. 락을 {@value #RESERVATION_LOCK_TIMEOUT_MS}ms까지만 기다린다 (D-047).
     * <p>
     * Hibernate는 PostgreSQL에서 이 힌트를 {@code SET LOCAL lock_timeout}으로 실행한다. 트랜잭션 안에서만 적용되고
     * 커밋/롤백 때 원래 값으로 돌아간다. 시간을 넘기면 {@code PessimisticLockingFailureException}이 나고,
     * Kafka 에러 핸들러가 일시 장애로 보고 재시도한다 (D-043).
     * <p>
     * 실험 1의 비관적 락 방식은 순수 대기 비용을 재려고 타임아웃 없는 {@link #findByProductIdForUpdate}를 그대로 쓴다 (D-017).
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = RESERVATION_LOCK_TIMEOUT_MS))
    @Query("select s from Stock s where s.productId = :productId")
    Optional<Stock> findByProductIdForReservation(@Param("productId") Long productId);
}
