package io.github.hbyun94.stock.domain;

/**
 * 예약 상태. RESERVED에서만 다른 상태로 갈 수 있다.
 * <pre>
 * RESERVED ──confirm──▶ CONFIRMED (출고 확정, 종료)
 *     └──────release──▶ RELEASED  (예약 복원, 종료)
 * </pre>
 */
public enum ReservationStatus {
    RESERVED,
    CONFIRMED,
    RELEASED
}
