package io.github.hbyun94.stock.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Stock 엔티티의 업무 규칙만 검증하는 순수 단위 테스트 (DB, Spring 없음) */
class StockTest {

    private static final Long PRODUCT_ID = 1L;

    @Test
    @DisplayName("생성 직후에는 예약이 없어 실물 재고가 곧 가용 재고다")
    void create() {
        Stock stock = Stock.create(PRODUCT_ID, 100);

        assertThat(stock.getOnHand()).isEqualTo(100);
        assertThat(stock.getReserved()).isZero();
        assertThat(stock.available()).isEqualTo(100);
    }

    @Test
    @DisplayName("초기 재고가 음수면 생성할 수 없다")
    void createWithNegativeQuantity() {
        assertThatThrownBy(() -> Stock.create(PRODUCT_ID, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Nested
    @DisplayName("예약")
    class Reserve {

        @Test
        @DisplayName("예약하면 가용 재고만 줄고 실물 재고는 그대로다")
        void reserve() {
            Stock stock = Stock.create(PRODUCT_ID, 10);

            stock.reserve(3);

            assertThat(stock.getOnHand()).isEqualTo(10);
            assertThat(stock.getReserved()).isEqualTo(3);
            assertThat(stock.available()).isEqualTo(7);
        }

        @Test
        @DisplayName("가용 재고를 정확히 다 써서 예약할 수 있다")
        void reserveAll() {
            Stock stock = Stock.create(PRODUCT_ID, 10);

            stock.reserve(10);

            assertThat(stock.available()).isZero();
        }

        @Test
        @DisplayName("가용 재고보다 많이 예약하면 예외가 나고 상태는 바뀌지 않는다")
        void reserveMoreThanAvailable() {
            Stock stock = Stock.create(PRODUCT_ID, 10);
            stock.reserve(8);

            assertThatThrownBy(() -> stock.reserve(3))
                    .isInstanceOf(InsufficientStockException.class)
                    .satisfies(e -> {
                        InsufficientStockException ex = (InsufficientStockException) e;
                        assertThat(ex.getRequested()).isEqualTo(3);
                        assertThat(ex.getAvailable()).isEqualTo(2);
                    });
            assertThat(stock.getReserved()).isEqualTo(8);
        }

        @Test
        @DisplayName("수량이 0 이하면 예약할 수 없다")
        void reserveNonPositive() {
            Stock stock = Stock.create(PRODUCT_ID, 10);

            assertThatThrownBy(() -> stock.reserve(0)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> stock.reserve(-1)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("출고 확정")
    class Confirm {

        @Test
        @DisplayName("확정하면 실물 재고와 예약 수량이 함께 줄고 가용 재고는 그대로다")
        void confirm() {
            Stock stock = Stock.create(PRODUCT_ID, 10);
            stock.reserve(3);

            stock.confirm(3);

            assertThat(stock.getOnHand()).isEqualTo(7);
            assertThat(stock.getReserved()).isZero();
            assertThat(stock.available()).isEqualTo(7);
        }

        @Test
        @DisplayName("예약된 수량보다 많이 확정할 수 없다")
        void confirmMoreThanReserved() {
            Stock stock = Stock.create(PRODUCT_ID, 10);
            stock.reserve(2);

            assertThatThrownBy(() -> stock.confirm(3)).isInstanceOf(IllegalStateException.class);
            assertThat(stock.getOnHand()).isEqualTo(10);
            assertThat(stock.getReserved()).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("예약 복원")
    class Release {

        @Test
        @DisplayName("복원하면 예약 수량이 줄어 가용 재고가 돌아온다")
        void release() {
            Stock stock = Stock.create(PRODUCT_ID, 10);
            stock.reserve(3);

            stock.release(3);

            assertThat(stock.getOnHand()).isEqualTo(10);
            assertThat(stock.getReserved()).isZero();
            assertThat(stock.available()).isEqualTo(10);
        }

        @Test
        @DisplayName("예약된 수량보다 많이 복원할 수 없다")
        void releaseMoreThanReserved() {
            Stock stock = Stock.create(PRODUCT_ID, 10);
            stock.reserve(2);

            assertThatThrownBy(() -> stock.release(3)).isInstanceOf(IllegalStateException.class);
            assertThat(stock.getReserved()).isEqualTo(2);
        }
    }
}
