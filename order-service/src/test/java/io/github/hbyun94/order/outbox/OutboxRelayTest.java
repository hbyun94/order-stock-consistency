package io.github.hbyun94.order.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.hbyun94.order.TestcontainersConfiguration;
import io.github.hbyun94.order.application.OrderEvents;
import io.github.hbyun94.order.domain.Order;
import io.github.hbyun94.order.domain.OrderRepository;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Outbox 저장과 발행기 동작을 검증한다.
 * 스케줄 발행기가 끼어들지 않도록 주기를 1시간으로 늘리고, 테스트가 relay()를 직접 호출한다.
 */
@SpringBootTest(properties = {
        "outbox.relay.interval-ms=3600000",
        "outbox.relay.send-timeout-ms=2000"
})
@Import(TestcontainersConfiguration.class)
class OutboxRelayTest {

    @Autowired
    OutboxRelay relay;

    @Autowired
    OutboxEventRepository outboxRepository;

    @Autowired
    OrderRepository orderRepository;

    @Autowired
    OrderEvents orderEvents;

    @Autowired
    PlatformTransactionManager transactionManager;

    @Autowired
    KafkaContainer kafka;

    TransactionTemplate tx;

    @BeforeEach
    void setUp() {
        relay.relay(); // 컨텍스트 기동 시 남은 행이 있으면 먼저 비운다
        outboxRepository.deleteAllInBatch();
        orderRepository.deleteAllInBatch();
        tx = new TransactionTemplate(transactionManager);
    }

    @Test
    @DisplayName("주문 트랜잭션이 롤백되면 Outbox 행도 남지 않는다 (함께 커밋되거나 함께 롤백)")
    void rollbackTogether() {
        tx.executeWithoutResult(status -> {
            Order order = orderRepository.save(newOrder());
            orderEvents.orderPlaced(order);
            status.setRollbackOnly();
        });

        assertThat(orderRepository.count()).isZero();
        assertThat(outboxRepository.count()).isZero();
    }

    @Test
    @DisplayName("트랜잭션 밖에서 이벤트를 저장하려 하면 예외가 난다 (MANDATORY: 주문과 따로 커밋되는 것을 막는다)")
    void requiresTransaction() {
        Order order = orderRepository.save(newOrder());

        assertThatThrownBy(() -> orderEvents.orderPlaced(order))
                .isInstanceOf(IllegalTransactionStateException.class);
    }

    @Test
    @DisplayName("한 발행기가 잠근 행은 다른 발행기가 SKIP LOCKED로 건너뛴다")
    void skipLocked() {
        saveOrderWithEvent();
        saveOrderWithEvent();

        TransactionTemplate other = new TransactionTemplate(transactionManager);
        other.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        tx.executeWithoutResult(status -> {
            List<OutboxEvent> lockedByFirst = outboxRepository.lockUnpublished(100);
            // 첫 번째가 락을 쥔 채로, 다른 트랜잭션(다른 커넥션)이 같은 쿼리를 실행한다
            List<OutboxEvent> seenBySecond = CompletableFuture.supplyAsync(
                    () -> other.execute(s -> outboxRepository.lockUnpublished(100))).join();

            assertThat(lockedByFirst).hasSize(2);
            assertThat(seenBySecond).isEmpty();
        });
    }

    @Test
    @DisplayName("브로커 장애 중에는 발행에 실패해도 행이 남고, 복구 후 다음 주기에 발행된다")
    void retryAfterBrokerOutage() {
        saveOrderWithEvent();
        pauseKafka();
        try {
            int published = relay.relay();

            assertThat(published).isZero();
            OutboxEvent failed = outboxRepository.findAll().getFirst();
            assertThat(failed.getPublishedAt()).isNull();
            assertThat(failed.getAttempts()).isEqualTo(1);
            assertThat(failed.getLastError()).isNotBlank();
        } finally {
            unpauseKafka();
        }

        int published = relay.relay();

        assertThat(published).isEqualTo(1);
        OutboxEvent sent = outboxRepository.findAll().getFirst();
        assertThat(sent.getPublishedAt()).isNotNull();
        assertThat(sent.getAttempts()).isEqualTo(2);
        assertThat(outboxRepository.countByPublishedAtIsNull()).isZero();
    }

    private void saveOrderWithEvent() {
        tx.executeWithoutResult(status -> orderEvents.orderPlaced(orderRepository.save(newOrder())));
    }

    private static Order newOrder() {
        return Order.place(UUID.randomUUID().toString(), "a".repeat(64), 1L, 1);
    }

    private void pauseKafka() {
        kafka.getDockerClient().pauseContainerCmd(kafka.getContainerId()).exec();
    }

    private void unpauseKafka() {
        kafka.getDockerClient().unpauseContainerCmd(kafka.getContainerId()).exec();
    }
}
