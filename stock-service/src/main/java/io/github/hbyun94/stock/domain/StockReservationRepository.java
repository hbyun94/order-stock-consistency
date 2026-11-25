package io.github.hbyun94.stock.domain;

import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface StockReservationRepository extends JpaRepository<StockReservation, Long> {

    Optional<StockReservation> findByOrderId(Long orderId);
}
