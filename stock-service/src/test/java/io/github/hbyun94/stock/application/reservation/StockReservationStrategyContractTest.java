package io.github.hbyun94.stock.application.reservation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.hbyun94.stock.TestcontainersConfiguration;
import io.github.hbyun94.stock.domain.InsufficientStockException;
import io.github.hbyun94.stock.domain.ReservationStatus;
import io.github.hbyun94.stock.domain.Stock;
import io.github.hbyun94.stock.domain.StockRepository;
import io.github.hbyun94.stock.domain.StockReservationRepository;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

/**
 * 모든 예약 전략이 같은 계약을 지키는지 검증한다 (단일 스레드, 정합성만 확인).
 * 컨텍스트에 등록된 전략 빈을 전부 대상으로 하므로, 전략을 추가하면 자동으로 검증 대상이 된다.
 * 동시성 비교는 별도 실험 테스트(StockReservationConcurrencyExperiment)에서 한다.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StockReservationStrategyContractTest {

    private static final Long PRODUCT_ID = 1L;

    @Autowired
    List<StockReservationStrategy> strategies;

    @Autowired
    StockRepository stockRepository;

    @Autowired
    StockReservationRepository reservationRepository;

    // 테스트 이름에 전략 이름이 보이도록 Named로 감싼다
    List<Named<StockReservationStrategy>> strategies() {
        return strategies.stream().map(s -> Named.of(s.name(), s)).toList();
    }

    @BeforeEach
    void setUp() {
        reservationRepository.deleteAllInBatch();
        stockRepository.deleteAllInBatch();
        stockRepository.save(Stock.create(PRODUCT_ID, 10));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("strategies")
    @DisplayName("예약하면 재고의 예약 수량과 예약 기록이 함께 저장된다")
    void reserve(StockReservationStrategy strategy) {
        strategy.reserve(100L, PRODUCT_ID, 3);

        assertThat(findStock().getReserved()).isEqualTo(3);
        assertThat(reservationRepository.findByOrderId(100L))
                .hasValueSatisfying(r -> assertThat(r.getStatus()).isEqualTo(ReservationStatus.RESERVED));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("strategies")
    @DisplayName("재고가 부족하면 예외가 나고 아무것도 저장되지 않는다")
    void reserveInsufficient(StockReservationStrategy strategy) {
        assertThatThrownBy(() -> strategy.reserve(100L, PRODUCT_ID, 11))
                .isInstanceOf(InsufficientStockException.class);

        assertThat(findStock().getReserved()).isZero();
        assertThat(reservationRepository.count()).isZero();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("strategies")
    @DisplayName("순차 요청이면 재고 수만큼만 성공하고 그다음은 재고 부족으로 실패한다")
    void reserveSequentiallyUntilSoldOut(StockReservationStrategy strategy) {
        for (long orderId = 1; orderId <= 10; orderId++) {
            strategy.reserve(orderId, PRODUCT_ID, 1);
        }

        assertThatThrownBy(() -> strategy.reserve(11L, PRODUCT_ID, 1))
                .isInstanceOf(InsufficientStockException.class);
        assertThat(findStock().getReserved()).isEqualTo(10);
        assertThat(reservationRepository.count()).isEqualTo(10);
    }

    private Stock findStock() {
        return stockRepository.findByProductId(PRODUCT_ID).orElseThrow();
    }
}
