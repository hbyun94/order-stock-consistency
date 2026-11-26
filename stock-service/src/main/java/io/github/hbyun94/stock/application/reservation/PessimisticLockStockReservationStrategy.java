package io.github.hbyun94.stock.application.reservation;

import io.github.hbyun94.stock.application.StockNotFoundException;
import io.github.hbyun94.stock.domain.Stock;
import io.github.hbyun94.stock.domain.StockRepository;
import io.github.hbyun94.stock.domain.StockReservation;
import io.github.hbyun94.stock.domain.StockReservationRepository;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 비관적 락: 재고 행을 읽는 시점에 DB 행 락을 잡는다.
 * <p>
 * 같은 상품을 예약하려는 트랜잭션들은 DB에서 한 줄로 서서 하나씩 처리된다.
 * 앞 트랜잭션이 커밋한 뒤에야 다음 트랜잭션이 최신 reserved 값을 읽으므로 lost update가 생기지 않는다.
 * 대가: 기다리는 동안 DB 커넥션을 붙잡고 있어서, 대기자가 커넥션 풀 크기만큼 쌓이면 다른 요청도 막힌다.
 */
@Component
public class PessimisticLockStockReservationStrategy implements StockReservationStrategy {

    private final StockRepository stockRepository;
    private final StockReservationRepository reservationRepository;

    public PessimisticLockStockReservationStrategy(
            StockRepository stockRepository, StockReservationRepository reservationRepository) {
        this.stockRepository = stockRepository;
        this.reservationRepository = reservationRepository;
    }

    @Override
    public String name() {
        return "비관적 락";
    }

    @Override
    @Transactional
    public StockReservation reserve(Long orderId, Long productId, int quantity) {
        Stock stock = stockRepository.findByProductIdForUpdate(productId) // 여기서 행 락 획득 (커밋 때 해제)
                .orElseThrow(() -> new StockNotFoundException(productId));
        stock.reserve(quantity);
        return reservationRepository.save(StockReservation.reserve(orderId, productId, quantity));
    }
}
