package io.github.hbyun94.order.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;

import io.github.hbyun94.order.TestcontainersConfiguration;
import io.github.hbyun94.order.domain.OrderRepository;
import io.github.hbyun94.order.idempotency.IdempotencyStore;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/** Redis 장애 시에도 주문은 받고(fail-open), 중복은 DB UNIQUE 제약이 막는지 검증한다 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class OrderServiceRedisDownTest {

    @MockitoBean
    IdempotencyStore idempotencyStore;

    @Autowired
    OrderService orderService;

    @Autowired
    OrderRepository orderRepository;

    @BeforeEach
    void redisIsDown() {
        orderRepository.deleteAllInBatch();
        given(idempotencyStore.tryAcquire(anyString(), anyString()))
                .willThrow(new RedisConnectionFailureException("Redis 장애 가정"));
    }

    @Test
    @DisplayName("Redis가 죽어도 주문은 저장되고, 같은 키의 재요청은 DB 제약으로 처음 주문을 돌려받는다")
    void failOpenWithDbGuard() {
        String key = UUID.randomUUID().toString();
        PlaceOrderCommand command = new PlaceOrderCommand(1L, 1);

        OrderResult first = orderService.placeOrder(key, command);
        OrderResult second = orderService.placeOrder(key, command);

        assertThat(first.replayed()).isFalse();
        assertThat(second.replayed()).isTrue();
        assertThat(second.order().getId()).isEqualTo(first.order().getId());
        assertThat(orderRepository.count()).isEqualTo(1);
    }
}
