package io.github.hbyun94.stock.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.hbyun94.stock.TestcontainersConfiguration;
import io.github.hbyun94.stock.domain.InsufficientStockException;
import io.github.hbyun94.stock.domain.ReservationStatus;
import io.github.hbyun94.stock.domain.Stock;
import io.github.hbyun94.stock.domain.StockRepository;
import io.github.hbyun94.stock.domain.StockReservationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;

/**
 * 실제 PostgreSQL에서 StockService의 트랜잭션 동작을 검증한다.
 * <p>
 * 테스트 메서드에 @Transactional을 붙이지 않는다. 붙이면 서비스가 테스트의 트랜잭션에 합류해
 * "예외 시 롤백", "커밋 후 DB 상태" 같은 실제 트랜잭션 경계를 검증할 수 없기 때문이다.
 * 대신 매 테스트 전에 테이블을 비운다.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class StockServiceTest {

    private static final Long PRODUCT_ID = 1L;

    @Autowired
    StockService stockService;

    @Autowired
    StockRepository stockRepository;

    @Autowired
    StockReservationRepository reservationRepository;

    @BeforeEach
    void setUp() {
        // 예약이 재고를 FK로 참조하므로 예약부터 지운다
        reservationRepository.deleteAllInBatch();
        stockRepository.deleteAllInBatch();
        stockRepository.save(Stock.create(PRODUCT_ID, 10));
    }

    @Test
    @DisplayName("예약하면 재고의 예약 수량과 예약 기록이 함께 저장된다")
    void reserve() {
        stockService.reserve(100L, PRODUCT_ID, 3);

        Stock stock = findStock();
        assertThat(stock.getOnHand()).isEqualTo(10);
        assertThat(stock.getReserved()).isEqualTo(3);
        assertThat(reservationRepository.findByOrderId(100L))
                .hasValueSatisfying(r -> {
                    assertThat(r.getStatus()).isEqualTo(ReservationStatus.RESERVED);
                    assertThat(r.getQuantity()).isEqualTo(3);
                });
    }

    @Test
    @DisplayName("재고가 부족하면 예외가 나고 재고도 예약 기록도 바뀌지 않는다")
    void reserveInsufficient() {
        stockService.reserve(100L, PRODUCT_ID, 8);

        assertThatThrownBy(() -> stockService.reserve(101L, PRODUCT_ID, 3))
                .isInstanceOf(InsufficientStockException.class);

        assertThat(findStock().getReserved()).isEqualTo(8);
        assertThat(reservationRepository.findByOrderId(101L)).isEmpty();
    }

    @Test
    @DisplayName("재고가 등록되지 않은 상품은 예약할 수 없다")
    void reserveUnknownProduct() {
        assertThatThrownBy(() -> stockService.reserve(100L, 999L, 1))
                .isInstanceOf(StockNotFoundException.class);
    }

    @Test
    @DisplayName("출고 확정하면 실물 재고가 차감되고 예약은 CONFIRMED가 된다")
    void confirm() {
        stockService.reserve(100L, PRODUCT_ID, 3);

        stockService.confirm(100L);

        Stock stock = findStock();
        assertThat(stock.getOnHand()).isEqualTo(7);
        assertThat(stock.getReserved()).isZero();
        assertThat(reservationRepository.findByOrderId(100L))
                .hasValueSatisfying(r -> assertThat(r.getStatus()).isEqualTo(ReservationStatus.CONFIRMED));
    }

    @Test
    @DisplayName("복원하면 가용 재고가 돌아오고 예약은 RELEASED가 된다")
    void release() {
        stockService.reserve(100L, PRODUCT_ID, 3);

        stockService.release(100L);

        Stock stock = findStock();
        assertThat(stock.getOnHand()).isEqualTo(10);
        assertThat(stock.available()).isEqualTo(10);
        assertThat(reservationRepository.findByOrderId(100L))
                .hasValueSatisfying(r -> assertThat(r.getStatus()).isEqualTo(ReservationStatus.RELEASED));
    }

    @Test
    @DisplayName("같은 주문을 두 번 확정하면 두 번째는 실패하고 재고는 한 번만 차감된다")
    void confirmTwice() {
        stockService.reserve(100L, PRODUCT_ID, 3);
        stockService.confirm(100L);

        assertThatThrownBy(() -> stockService.confirm(100L)).isInstanceOf(IllegalStateException.class);

        assertThat(findStock().getOnHand()).isEqualTo(7);
    }

    @Test
    @DisplayName("예약이 없는 주문은 확정/복원할 수 없다")
    void confirmUnknownOrder() {
        assertThatThrownBy(() -> stockService.confirm(999L)).isInstanceOf(ReservationNotFoundException.class);
        assertThatThrownBy(() -> stockService.release(999L)).isInstanceOf(ReservationNotFoundException.class);
    }

    @Test
    @DisplayName("[현재 동작] 같은 주문을 두 번 예약하면 order_id UNIQUE 제약 위반으로 실패하고 두 번째 예약은 롤백된다")
    void reserveSameOrderTwice() {
        // 6단계(중복 소비)에서 이 동작을 "예외"가 아니라 "멱등하게 무시"로 바꿀지 결정한다
        stockService.reserve(100L, PRODUCT_ID, 3);

        assertThatThrownBy(() -> stockService.reserve(100L, PRODUCT_ID, 3))
                .isInstanceOf(DataIntegrityViolationException.class);

        // 두 번째 트랜잭션의 reserved 증가도 함께 롤백됐다
        assertThat(findStock().getReserved()).isEqualTo(3);
        assertThat(reservationRepository.count()).isEqualTo(1);
    }

    private Stock findStock() {
        return stockRepository.findByProductId(PRODUCT_ID).orElseThrow();
    }
}
