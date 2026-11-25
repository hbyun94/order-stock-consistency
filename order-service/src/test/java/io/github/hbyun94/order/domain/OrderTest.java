package io.github.hbyun94.order.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Order 생성 규칙을 검증하는 순수 단위 테스트 */
class OrderTest {

    private static final String KEY = "key-1";
    private static final String FINGERPRINT = "a".repeat(64);

    @Test
    @DisplayName("새 주문은 PENDING 상태로 만들어진다")
    void place() {
        Order order = Order.place(KEY, FINGERPRINT, 1L, 2);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING);
        assertThat(order.getIdempotencyKey()).isEqualTo(KEY);
        assertThat(order.getQuantity()).isEqualTo(2);
    }

    @Test
    @DisplayName("멱등 키가 비어 있으면 주문할 수 없다")
    void placeWithoutKey() {
        assertThatThrownBy(() -> Order.place(" ", FINGERPRINT, 1L, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("요청 지문이 SHA-256 16진수 64자가 아니면 주문할 수 없다")
    void placeWithInvalidFingerprint() {
        assertThatThrownBy(() -> Order.place(KEY, "abc", 1L, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("수량이 0 이하면 주문할 수 없다")
    void placeWithNonPositiveQuantity() {
        assertThatThrownBy(() -> Order.place(KEY, FINGERPRINT, 1L, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
