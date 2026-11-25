package io.github.hbyun94.order.domain;

import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, Long> {

    Optional<Order> findByIdempotencyKey(String idempotencyKey);
}
