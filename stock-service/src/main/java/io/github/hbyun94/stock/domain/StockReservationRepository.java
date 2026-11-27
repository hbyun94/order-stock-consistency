package io.github.hbyun94.stock.domain;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;

public interface StockReservationRepository extends JpaRepository<StockReservation, Long> {

    Optional<StockReservation> findByOrderId(Long orderId);

    /**
     * 확정·복원용 비관적 락 조회 (D-048). 같은 주문을 동시에 두 번 확정해도 두 번째 요청은 첫 번째가 끝날 때까지 기다린 뒤
     * 바뀐 상태(CONFIRMED)를 보고 실패한다. 재고 조회와 같은 3초 대기 상한을 둔다 (D-047).
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = StockRepository.RESERVATION_LOCK_TIMEOUT_MS))
    @Query("select r from StockReservation r where r.orderId = :orderId")
    Optional<StockReservation> findByOrderIdForUpdate(@Param("orderId") Long orderId);
}
