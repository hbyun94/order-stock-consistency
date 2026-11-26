package io.github.hbyun94.order.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.hbyun94.order.TestcontainersConfiguration;
import io.github.hbyun94.order.domain.OrderRepository;
import io.github.hbyun94.order.domain.OrderStatus;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;

/** 실제 PostgreSQL + Redis에서 주문 멱등 처리를 검증한다 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class OrderServiceTest {

    private static final PlaceOrderCommand COMMAND = new PlaceOrderCommand(1L, 2);

    @Autowired
    OrderService orderService;

    @Autowired
    OrderRepository orderRepository;

    @Autowired
    StringRedisTemplate redis;

    String key;

    @BeforeEach
    void setUp() {
        orderRepository.deleteAllInBatch();
        key = UUID.randomUUID().toString();
    }

    @Test
    @DisplayName("새 키로 주문하면 PENDING 주문이 저장된다")
    void placeOrder() {
        OrderResult result = orderService.placeOrder(key, COMMAND);

        assertThat(result.replayed()).isFalse();
        assertThat(result.order().getStatus()).isEqualTo(OrderStatus.PENDING);
        assertThat(orderRepository.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("같은 키로 다시 요청하면 새 주문 없이 처음 주문을 그대로 돌려준다 (Redis 1차 차단)")
    void replaySameKey() {
        Long firstId = orderService.placeOrder(key, COMMAND).order().getId();

        OrderResult second = orderService.placeOrder(key, COMMAND);

        assertThat(second.replayed()).isTrue();
        assertThat(second.order().getId()).isEqualTo(firstId);
        assertThat(orderRepository.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("같은 키로 내용이 다른 요청을 보내면 거절한다")
    void rejectDifferentRequestWithSameKey() {
        orderService.placeOrder(key, COMMAND);

        assertThatThrownBy(() -> orderService.placeOrder(key, new PlaceOrderCommand(1L, 3)))
                .isInstanceOf(IdempotencyKeyReusedException.class);
        assertThat(orderRepository.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("같은 키로 동시에 10번 요청해도 주문은 1건이고, 성공 응답은 모두 같은 주문 ID다")
    void concurrentSameKey() throws Exception {
        int threads = 10;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<OrderResult>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                return orderService.placeOrder(key, COMMAND);
            }));
        }
        start.countDown();

        Set<Long> orderIds = new HashSet<>();
        AtomicInteger inProgress = new AtomicInteger();
        for (Future<OrderResult> future : futures) {
            try {
                orderIds.add(future.get().order().getId());
            } catch (java.util.concurrent.ExecutionException e) {
                assertThat(e.getCause()).isInstanceOf(IdempotentRequestInProgressException.class);
                inProgress.incrementAndGet();
            }
        }
        pool.shutdown();

        assertThat(orderRepository.count()).isEqualTo(1);
        assertThat(orderIds).hasSize(1);
        System.out.printf("[멱등] 동시 10건: 성공 응답 %d건(같은 주문 ID), 처리 중(409) %d건%n",
                threads - inProgress.get(), inProgress.get());
    }

    @Test
    @DisplayName("Redis 키가 사라져도(만료/유실) DB UNIQUE 제약이 중복 주문을 막고 처음 주문을 돌려준다")
    void dbUniqueConstraintIsLastLineOfDefense() {
        Long firstId = orderService.placeOrder(key, COMMAND).order().getId();
        redis.delete("idempotency:order:" + key); // Redis 키 유실 상황

        OrderResult second = orderService.placeOrder(key, COMMAND);

        assertThat(second.replayed()).isTrue();
        assertThat(second.order().getId()).isEqualTo(firstId);
        assertThat(orderRepository.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("Redis 키가 사라진 뒤 같은 키로 내용이 다른 요청이 오면 DB에 저장된 지문으로 거절한다")
    void dbFingerprintRejectsDifferentRequest() {
        orderService.placeOrder(key, COMMAND);
        redis.delete("idempotency:order:" + key);

        assertThatThrownBy(() -> orderService.placeOrder(key, new PlaceOrderCommand(1L, 3)))
                .isInstanceOf(IdempotencyKeyReusedException.class);
        assertThat(orderRepository.count()).isEqualTo(1);
        // 거절하면서 자신이 선점했던 Redis 키를 풀었으므로 남은 키가 없다
        assertThat(redis.hasKey("idempotency:order:" + key)).isFalse();
    }

    @Test
    @DisplayName("다른 키면 같은 내용이라도 별개의 주문이다 (사용자가 정말로 두 번 주문한 경우)")
    void differentKeysAreDifferentOrders() {
        orderService.placeOrder(key, COMMAND);
        orderService.placeOrder(UUID.randomUUID().toString(), COMMAND);

        assertThat(orderRepository.count()).isEqualTo(2);
    }
}
