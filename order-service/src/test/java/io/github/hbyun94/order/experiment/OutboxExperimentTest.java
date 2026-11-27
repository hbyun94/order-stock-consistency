package io.github.hbyun94.order.experiment;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hbyun94.order.TestcontainersConfiguration;
import io.github.hbyun94.order.application.OrderService;
import io.github.hbyun94.order.application.PlaceOrderCommand;
import io.github.hbyun94.order.domain.Order;
import io.github.hbyun94.order.domain.OrderRepository;
import io.github.hbyun94.order.messaging.KafkaTopics;
import io.github.hbyun94.order.messaging.OrderCreatedEvent;
import io.github.hbyun94.order.outbox.OutboxEvent;
import io.github.hbyun94.order.outbox.OutboxEventRepository;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.json.JsonMapper;

/**
 * 실험 3: 주문 저장 후 이벤트 발행이 실패하면 — 커밋 후 직접 전송(5단계) vs Transactional Outbox(7단계).
 * <p>
 * 일반 test에 포함된다. 결과: 콘솔과 {@code order-service/build/experiment/experiment-3.md}
 * <p>
 * 시나리오(각 주문 20건)
 * <ol>
 *   <li>정상: 장애 없음</li>
 *   <li>브로커 장애: Kafka 컨테이너를 실제로 6초간 일시정지한 동안 주문한다</li>
 *   <li>커밋 직후 프로세스 종료: 주문 커밋 뒤 전송 단계가 실행되지 못한다(재시작 전 상태를 흉내)</li>
 * </ol>
 * 단정은 결정적인 사실만 검사한다: 정상 시 모두 도착, 커밋 직후 종료 시 직접 전송 전부 유실, Outbox는 모든 시나리오에서 유실 0,
 * 장애 중 직접 전송은 프로듀서가 실패로 알림. 장애 중 직접 전송의 실제 도착 수는 환경에 따라 달라 기록만 한다 (D-049).
 * <p>
 * 프로듀서 전송 제한 시간(delivery.timeout.ms)을 3초로 줄였다(기본 120초). "장애가 전송 제한 시간보다 길다"는
 * 조건을 짧은 실험 시간 안에 만들기 위해서다. 기본값이어도 장애가 2분을 넘기면 같은 일이 생긴다.
 */
@SpringBootTest(properties = {
        "spring.kafka.producer.properties.delivery.timeout.ms=3000",
        "spring.kafka.producer.properties.request.timeout.ms=1000",
        "spring.kafka.producer.properties.max.block.ms=2000",
        "outbox.relay.send-timeout-ms=3000",
        "outbox.relay.interval-ms=500"
})
@Import(TestcontainersConfiguration.class)
class OutboxExperimentTest {

    private static final int ORDERS = 20;
    private static final Duration OUTAGE = Duration.ofSeconds(6);
    private static final Duration DELIVERY_WAIT = Duration.ofSeconds(20);

    @Autowired
    OrderService orderService;

    @Autowired
    OrderRepository orderRepository;

    @Autowired
    OutboxEventRepository outboxRepository;

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    TransactionTemplate transactionTemplate;

    @Autowired
    ConsumerFactory<String, String> consumerFactory;

    @Autowired
    JsonMapper jsonMapper;

    @Autowired
    KafkaContainer kafka;

    Consumer<String, String> consumer;
    final ConcurrentHashMap<Long, Integer> received = new ConcurrentHashMap<>();
    /** 직접 전송에서 프로듀서가 "실패"라고 알려 온 건수 (장애 시나리오) */
    final AtomicInteger directSendReportedFailures = new AtomicInteger();
    /** 정상 시나리오에서 Outbox로 넣은 주문 ID (지연 측정 대상) */
    Set<Long> outboxNormalOrderIds = Set.of();

    @BeforeEach
    void subscribe() {
        consumer = consumerFactory.createConsumer("experiment-" + UUID.randomUUID(), null);
        consumer.subscribe(List.of(KafkaTopics.ORDER_CREATED));
    }

    @AfterEach
    void close() {
        consumer.close();
    }

    @Test
    void compareDirectSendAndOutbox() throws IOException {
        List<Row> rows = new ArrayList<>();
        int directNormal = normal(this::placeDirect);
        directSendReportedFailures.set(0);
        int directOutage = outage(this::placeDirect);
        int directReportedFailures = directSendReportedFailures.get();
        rows.add(new Row("커밋 후 직접 전송 (5단계)", directNormal, directOutage, crashAfterCommit(this::placeDirectWithoutSend)));

        Set<Long> normalIds = placeMany(this::placeWithOutbox);
        outboxNormalOrderIds = normalIds;
        int outboxNormal = awaitDelivered(normalIds);
        rows.add(new Row("Transactional Outbox (7단계)",
                outboxNormal, outage(this::placeWithOutbox), crashAfterCommit(this::placeWithOutbox)));
        LongSummaryStatistics latency = outboxLatency();

        String report = report(rows, latency, directReportedFailures);
        System.out.println(report);
        Path out = Path.of("build/experiment/experiment-3.md");
        Files.createDirectories(out.getParent());
        Files.writeString(out, report);

        Row direct = rows.get(0);
        Row outbox = rows.get(1);
        assertThat(direct.normal()).isEqualTo(ORDERS);
        // 장애 중 직접 전송이 실제로 몇 건 도착하는지는 타이밍에 따라 0~20건으로 달라진다(로컬 7~13건 유실, CI 한 번은 0건).
        // 소켓까지 나간 요청은 브로커 재개 후 처리되고, 프로듀서 버퍼에 남은 요청만 만료되기 때문이다. 그래서 도착 수는 기록만 한다.
        // 결정적인 사실은 "프로듀서가 실패로 알렸다"는 것이다: 보낸 쪽은 결과를 모른다(재전송하면 중복, 안 하면 유실 가능)
        assertThat(directReportedFailures).as("직접 전송은 장애 중 전송을 실패로 알린다(실제 도착 여부는 모름)").isPositive();
        assertThat(direct.crash()).as("커밋 후 전송 전에 죽으면 이벤트가 없다").isZero();
        assertThat(outbox.normal()).isEqualTo(ORDERS);
        assertThat(outbox.outage()).as("Outbox는 장애가 끝나면 모두 발행한다").isEqualTo(ORDERS);
        assertThat(outbox.crash()).as("Outbox 행이 커밋돼 있으므로 발행기가 보낸다").isEqualTo(ORDERS);
        assertThat(outboxRepository.countByPublishedAtIsNull()).isZero();
    }

    // ── 시나리오: 각 시나리오는 주문 20건을 넣고, 도착한 order.created 수(주문 ID 기준)를 돌려준다 ──────

    private int normal(Supplier<Long> placeOne) {
        Set<Long> orderIds = placeMany(placeOne);
        return awaitDelivered(orderIds);
    }

    private int outage(Supplier<Long> placeOne) {
        pauseKafka();
        Set<Long> orderIds;
        try {
            orderIds = placeMany(placeOne); // 장애 중 주문: 주문 자체는 DB에 정상 커밋된다
            sleep(OUTAGE);
        } finally {
            unpauseKafka();
        }
        return awaitDelivered(orderIds);
    }

    private int crashAfterCommit(Supplier<Long> placeOne) {
        return awaitDelivered(placeMany(placeOne));
    }

    private Set<Long> placeMany(Supplier<Long> placeOne) {
        Set<Long> orderIds = new HashSet<>();
        for (int i = 0; i < ORDERS; i++) {
            orderIds.add(placeOne.get());
        }
        return orderIds;
    }

    // ── 방식 ─────────────────────────────────────────────────────────────

    /** 5단계 방식: 주문 커밋 후 Kafka로 바로 보낸다. 실패하면 로그만 남는다(재시도 없음). */
    private Long placeDirect() {
        Order order = saveOrderOnly();
        try {
            kafkaTemplate.send(KafkaTopics.ORDER_CREATED, String.valueOf(order.getProductId()),
                            new OrderCreatedEvent(order.getId(), order.getProductId(), order.getQuantity(), Instant.now()))
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            directSendReportedFailures.incrementAndGet();
                        }
                    });
        } catch (RuntimeException e) {
            directSendReportedFailures.incrementAndGet();
            // 5단계 OrderCreatedPublisher와 같다: 커밋 뒤라 되돌릴 수 없으므로 기록만 하고 넘어간다
        }
        return order.getId();
    }

    /** 5단계 방식에서 커밋 직후 프로세스가 죽은 경우: 전송 코드에 도달하지 못한다. */
    private Long placeDirectWithoutSend() {
        return saveOrderOnly().getId();
    }

    /** 7단계 방식: 주문과 Outbox 행을 한 트랜잭션에 저장한다. 전송은 발행기가 한다.
     *  "커밋 직후 종료" 시나리오에서도 같은 코드다: 커밋된 Outbox 행은 재시작한 발행기가 보낸다. */
    private Long placeWithOutbox() {
        return orderService.placeOrder(UUID.randomUUID().toString(), new PlaceOrderCommand(1L, 1)).order().getId();
    }

    private Order saveOrderOnly() {
        return transactionTemplate.execute(status -> orderRepository.save(
                Order.place(UUID.randomUUID().toString(), "a".repeat(64), 1L, 1)));
    }

    // ── 측정 ─────────────────────────────────────────────────────────────

    /** 주어진 주문들의 order.created가 몇 건 도착했는지 센다. 모두 오거나 대기 시간이 지나면 멈춘다. */
    private int awaitDelivered(Set<Long> orderIds) {
        long deadline = System.nanoTime() + DELIVERY_WAIT.toNanos();
        while (System.nanoTime() < deadline && delivered(orderIds) < orderIds.size()) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
                received.merge(jsonMapper.readTree(record.value()).get("orderId").asLong(), 1, Integer::sum);
            }
        }
        return delivered(orderIds);
    }

    private int delivered(Set<Long> orderIds) {
        return (int) orderIds.stream().filter(received::containsKey).count();
    }

    /** 정상 시나리오 Outbox 행의 커밋 → 발행 지연(ms) */
    private LongSummaryStatistics outboxLatency() {
        return outboxRepository.findAll().stream()
                .filter(e -> e.getPublishedAt() != null)
                .filter(e -> outboxNormalOrderIds.contains(jsonMapper.readTree(e.getPayload()).get("orderId").asLong()))
                .mapToLong(e -> Duration.between(e.getCreatedAt(), e.getPublishedAt()).toMillis())
                .summaryStatistics();
    }

    record Row(String approach, int normal, int outage, int crash) {
    }

    private static String report(List<Row> rows, LongSummaryStatistics latency, int directReportedFailures) {
        StringBuilder sb = new StringBuilder();
        sb.append("## 실험 3 결과 (").append(LocalDate.now()).append(")\n\n");
        sb.append("- 시나리오마다 주문 ").append(ORDERS).append("건. 값 = 도착한 order.created 수 / 주문 수\n");
        sb.append("- 브로커 장애: Kafka 컨테이너 ").append(OUTAGE.toSeconds())
                .append("초 일시정지. 프로듀서 전송 제한 시간 3초(실험용 단축, 기본 120초)\n\n");
        sb.append("| 방식 | 정상 | 브로커 장애 중 주문 | 커밋 직후 프로세스 종료 | 유실 합계 |\n");
        sb.append("|---|---|---|---|---|\n");
        for (Row r : rows) {
            int lost = (ORDERS - r.normal()) + (ORDERS - r.outage()) + (ORDERS - r.crash());
            sb.append("| ").append(r.approach())
                    .append(" | ").append(r.normal()).append(" / ").append(ORDERS)
                    .append(" | ").append(r.outage()).append(" / ").append(ORDERS)
                    .append(" | ").append(r.crash()).append(" / ").append(ORDERS)
                    .append(" | ").append(lost)
                    .append(" |\n");
        }
        sb.append("\n- 직접 전송, 브로커 장애 중: 프로듀서가 실패로 알린 전송 ").append(directReportedFailures).append("건 / ")
                .append(ORDERS).append("건 (도착 수와 함께 해석: 실패로 알렸어도 브로커에는 기록됐을 수 있다)\n");
        sb.append("- Outbox 커밋 → 발행 지연(정상 시나리오 ").append(latency.getCount()).append("건): 평균 ")
                .append(Math.round(latency.getAverage())).append("ms, 최대 ").append(latency.getMax()).append("ms\n");
        return sb.toString();
    }

    private void pauseKafka() {
        kafka.getDockerClient().pauseContainerCmd(kafka.getContainerId()).exec();
    }

    private void unpauseKafka() {
        kafka.getDockerClient().unpauseContainerCmd(kafka.getContainerId()).exec();
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
