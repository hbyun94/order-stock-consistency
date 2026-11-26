package io.github.hbyun94.stock.domain;

import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface VersionedStockRepository extends JpaRepository<VersionedStock, Long> {

    Optional<VersionedStock> findByProductId(Long productId);
}
