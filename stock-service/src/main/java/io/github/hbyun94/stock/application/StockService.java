package io.github.hbyun94.stock.application;

import io.github.hbyun94.stock.domain.Stock;
import io.github.hbyun94.stock.domain.StockRepository;
import io.github.hbyun94.stock.domain.StockReservation;
import io.github.hbyun94.stock.domain.StockReservationRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 재고 예약/확정/복원 유스케이스.
 * <p>
 * 업무 규칙(재고 부족, 상태 전이)은 엔티티가 검사하고, 이 클래스는 트랜잭션 경계만 정한다.
 * 메서드 하나 = 트랜잭션 하나. Stock 변경과 StockReservation 변경은 함께 커밋되거나 함께 롤백된다.
 * <p>
 * 락이 없으므로 같은 상품을 동시에 예약하면 lost update가 생길 수 있다 (3단계 실험 1의 기준선).
 */
@Service
public class StockService {

    private final StockRepository stockRepository;
    private final StockReservationRepository reservationRepository;

    public StockService(StockRepository stockRepository, StockReservationRepository reservationRepository) {
        this.stockRepository = stockRepository;
        this.reservationRepository = reservationRepository;
    }

    /**
     * 주문 수량만큼 재고를 예약한다.
     *
     * @throws io.github.hbyun94.stock.domain.InsufficientStockException 가용 재고 부족 (아무것도 저장되지 않음)
     */
    @Transactional
    public StockReservation reserve(Long orderId, Long productId, int quantity) {
        Stock stock = getStock(productId);
        stock.reserve(quantity); // 부족하면 여기서 예외 → 트랜잭션 롤백
        // Stock 변경은 save 호출 없이 커밋 시점의 변경 감지(dirty checking)로 UPDATE된다
        return reservationRepository.save(StockReservation.reserve(orderId, productId, quantity));
    }

    /** 출고 확정: 예약 상태를 CONFIRMED로 바꾸고 실물 재고를 차감한다. */
    @Transactional
    public void confirm(Long orderId) {
        StockReservation reservation = getReservation(orderId);
        reservation.confirm(); // 상태 검사를 먼저 해서, 잘못된 요청이면 재고를 건드리기 전에 실패시킨다
        getStock(reservation.getProductId()).confirm(reservation.getQuantity());
    }

    /** 예약 복원: 예약 상태를 RELEASED로 바꾸고 가용 재고를 되돌린다. */
    @Transactional
    public void release(Long orderId) {
        StockReservation reservation = getReservation(orderId);
        reservation.release();
        getStock(reservation.getProductId()).release(reservation.getQuantity());
    }

    private Stock getStock(Long productId) {
        return stockRepository.findByProductId(productId)
                .orElseThrow(() -> new StockNotFoundException(productId));
    }

    private StockReservation getReservation(Long orderId) {
        return reservationRepository.findByOrderId(orderId)
                .orElseThrow(() -> new ReservationNotFoundException(orderId));
    }
}
