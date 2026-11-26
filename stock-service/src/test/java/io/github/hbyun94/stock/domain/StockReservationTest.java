package io.github.hbyun94.stock.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** StockReservation의 상태 전이 규칙을 검증하는 순수 단위 테스트 */
class StockReservationTest {

    @Test
    @DisplayName("예약을 만들면 RESERVED 상태다")
    void reserve() {
        StockReservation reservation = StockReservation.reserve(10L, 1L, 3);

        assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.RESERVED);
        assertThat(reservation.getQuantity()).isEqualTo(3);
    }

    @Test
    @DisplayName("수량이 0 이하인 예약은 만들 수 없다")
    void reserveNonPositive() {
        assertThatThrownBy(() -> StockReservation.reserve(10L, 1L, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("RESERVED → CONFIRMED")
    void confirm() {
        StockReservation reservation = StockReservation.reserve(10L, 1L, 3);

        reservation.confirm();

        assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.CONFIRMED);
    }

    @Test
    @DisplayName("RESERVED → RELEASED")
    void release() {
        StockReservation reservation = StockReservation.reserve(10L, 1L, 3);

        reservation.release();

        assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.RELEASED);
    }

    @Test
    @DisplayName("같은 예약을 두 번 확정할 수 없다")
    void confirmTwice() {
        StockReservation reservation = StockReservation.reserve(10L, 1L, 3);
        reservation.confirm();

        assertThatThrownBy(reservation::confirm).isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("출고 확정된 예약은 복원할 수 없다")
    void releaseAfterConfirm() {
        StockReservation reservation = StockReservation.reserve(10L, 1L, 3);
        reservation.confirm();

        assertThatThrownBy(reservation::release).isInstanceOf(IllegalStateException.class);
        assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.CONFIRMED);
    }

    @Test
    @DisplayName("복원된 예약은 다시 복원하거나 확정할 수 없다")
    void actAfterRelease() {
        StockReservation reservation = StockReservation.reserve(10L, 1L, 3);
        reservation.release();

        assertThatThrownBy(reservation::release).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(reservation::confirm).isInstanceOf(IllegalStateException.class);
    }
}
