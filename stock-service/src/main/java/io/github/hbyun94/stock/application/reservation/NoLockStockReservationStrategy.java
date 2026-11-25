package io.github.hbyun94.stock.application.reservation;

import io.github.hbyun94.stock.application.StockService;
import io.github.hbyun94.stock.domain.StockReservation;
import org.springframework.stereotype.Component;

/**
 * 락 없음 (실험 1의 기준선).
 * <p>
 * 조회 → 메모리에서 reserved 증가 → 커밋 시 {@code UPDATE stock SET reserved = <계산한 값>}.
 * 두 트랜잭션이 같은 reserved 값을 읽으면 뒤에 커밋한 쪽이 앞의 변경을 덮어쓴다 (lost update).
 * 예약 기록은 둘 다 저장되므로 실제 예약 합계가 재고보다 많아진다 = 초과 판매.
 */
@Component
public class NoLockStockReservationStrategy implements StockReservationStrategy {

    private final StockService stockService;

    public NoLockStockReservationStrategy(StockService stockService) {
        this.stockService = stockService;
    }

    @Override
    public String name() {
        return "락 없음";
    }

    @Override
    public StockReservation reserve(Long orderId, Long productId, int quantity) {
        return stockService.reserve(orderId, productId, quantity);
    }
}
