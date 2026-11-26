package io.github.hbyun94.stock.experiment;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hbyun94.stock.TestcontainersConfiguration;
import io.github.hbyun94.stock.application.OrderReservationService;
import io.github.hbyun94.stock.application.StockNotFoundException;
import io.github.hbyun94.stock.application.StockService;
import io.github.hbyun94.stock.application.reservation.PessimisticLockStockReservationStrategy;
import io.github.hbyun94.stock.domain.InsufficientStockException;
import io.github.hbyun94.stock.domain.OrderEventResultRepository;
import io.github.hbyun94.stock.domain.ReservationOutcome;
import io.github.hbyun94.stock.domain.Stock;
import io.github.hbyun94.stock.domain.StockRepository;
import io.github.hbyun94.stock.domain.StockReservationRepository;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 실험 2: 같은 이벤트를 두 번 소비하면 어떻게 되는가 — 처리 방식 3가지 비교.
 * <p>
 * 일반 test에 포함된다(몇 초, 결과가 결정적). 결과: 콘솔과 {@code stock-service/build/experiment/experiment-2.md}
 * <p>
 * 방식
 * <ul>
 *   <li>기준선: 주문별 기록 없이 재고 숫자만 줄인다 (테스트에만 있는 코드)</li>
 *   <li>5단계 방식: 예약 기록(order_id UNIQUE)만 있고, 중복이면 예외 (5단계 OrderCreatedListener와 같은 처리)</li>
 *   <li>멱등 처리: 처리 결과 기록 후 중복이면 기록된 결과를 다시 보냄 (D-033, 현재 코드)</li>
 * </ul>
 * 시나리오
 * <ol>
 *   <li>같은 order.created가 두 번 전달된다 (커밋 전 재시작, 리밸런싱 등)</li>
 *   <li>예약 후 결과 전송이 한 번 실패해 같은 메시지가 재전달된다 (이 프로젝트의 실제 재전달 경로)</li>
 *   <li>재고 부족으로 거절된 주문이, 그 사이 재고가 풀린 뒤 다시 전달된다</li>
 * </ol>
 * Kafka 재전달은 Spring Kafka 기본 동작(예외 시 같은 메시지를 최대 10번 시도)을 흉내 낸다.
 * 메시지 전달 계층 자체는 OrderCreatedListenerTest, OrderCreatedRedeliveryTest가 실제 Kafka로 검증한다.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class DuplicateConsumptionExperimentTest {

    private static final Long PRODUCT_ID = 1L;
    private static final int QUANTITY = 2;
    private static final int MAX_ATTEMPTS = 10; // Spring Kafka DefaultErrorHandler 기본값: 1회 + 재시도 9회

    @Autowired
    StockRepository stockRepository;

    @Autowired
    StockReservationRepository reservationRepository;

    @Autowired
    OrderEventResultRepository resultRepository;

    @Autowired
    StockService stockService;

    @Autowired
    PessimisticLockStockReservationStrategy pessimisticStrategy;

    @Autowired
    OrderReservationService reservationService;

    @Autowired
    TransactionTemplate transactionTemplate;

    /** order.created 한 건을 처리하고, 보낼 결과("RESERVED"/"REJECTED")를 돌려준다. 실패하면 예외. */
    interface Approach {
        String name();

        String process(long orderId, long productId, int quantity);
    }

    @Test
    void compareDuplicateConsumption() throws IOException {
        List<Approach> approaches = List.of(naive(), step5(), idempotent());
        List<Row> rows = new ArrayList<>();
        for (Approach approach : approaches) {
            rows.add(new Row(approach.name(), scenario1(approach), scenario2(approach), scenario3(approach)));
        }

        String report = report(rows);
        System.out.println(report);
        Path out = Path.of("build/experiment/experiment-2.md");
        Files.createDirectories(out.getParent());
        Files.writeString(out, report);

        Row naive = rows.get(0);
        Row step5 = rows.get(1);
        Row idempotent = rows.get(2);
        // 기준선은 중복 차감이 재현된다
        assertThat(naive.s1().reserved()).isEqualTo(QUANTITY * 2);
        // 5단계 방식은 차감은 1번이지만, 결과 전송 실패 후 재전달이 계속 실패해 결과가 나가지 않는다 (주문 PENDING 고정)
        assertThat(step5.s1().reserved()).isEqualTo(QUANTITY);
        assertThat(step5.s2().resultsSent()).isZero();
        assertThat(step5.s3().reserved()).isEqualTo(QUANTITY); // 거절됐던 주문이 재고를 잡음 (누수)
        // 멱등 처리는 모든 시나리오에서 차감 1번, 결과 도달, 누수 없음
        assertThat(idempotent.s1().reserved()).isEqualTo(QUANTITY);
        assertThat(idempotent.s1().resultsSent()).isEqualTo(2);
        assertThat(idempotent.s1().gaveUp()).isZero();
        assertThat(idempotent.s2().reserved()).isEqualTo(QUANTITY);
        assertThat(idempotent.s2().resultsSent()).isEqualTo(1);
        assertThat(idempotent.s3().reserved()).isZero();
        assertThat(idempotent.s3().redeliveredOutcome()).isEqualTo("REJECTED");
    }

    // ── 시나리오 ─────────────────────────────────────────────────────────

    /** ① 같은 이벤트가 두 번 전달된다 */
    private Measure scenario1(Approach approach) {
        reset(10);
        Delivery first = deliver(approach, 100L, QUANTITY, false);
        Delivery second = deliver(approach, 100L, QUANTITY, false);
        return measure(first.sent() + second.sent(), first.gaveUp() + second.gaveUp(), first.failures() + second.failures(), null);
    }

    /** ② 결과 전송이 한 번 실패해 같은 메시지가 재전달된다 */
    private Measure scenario2(Approach approach) {
        reset(10);
        Delivery delivery = deliver(approach, 100L, QUANTITY, true);
        return measure(delivery.sent(), delivery.gaveUp(), delivery.failures(), null);
    }

    /** ③ 거절된 주문이, 그 사이 재고가 풀린 뒤 다시 전달된다 */
    private Measure scenario3(Approach approach) {
        reset(2);
        stockService.reserve(50L, PRODUCT_ID, 1);                 // 다른 주문이 1개 예약 → 가용 1
        Delivery rejected = deliver(approach, 100L, QUANTITY, false); // 2개 요청 → 거절
        stockService.release(50L);                                // 다른 주문 취소 → 가용 2
        Delivery redelivered = deliver(approach, 100L, QUANTITY, false);
        return measure(rejected.sent() + redelivered.sent(), redelivered.gaveUp(), redelivered.failures(),
                redelivered.lastOutcome());
    }

    // ── 전달 흉내 ────────────────────────────────────────────────────────

    record Delivery(int sent, int failures, int gaveUp, String lastOutcome) {
    }

    /**
     * 메시지 하나를 전달한다. 처리 중 예외가 나면(전송 실패 포함) 같은 메시지를 최대 10번까지 다시 처리한다.
     *
     * @param failFirstSend true면 첫 번째 결과 전송을 실패시킨다 (예약은 이미 커밋된 뒤)
     */
    private Delivery deliver(Approach approach, long orderId, int quantity, boolean failFirstSend) {
        int failures = 0;
        boolean sendFailed = false;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                String outcome = approach.process(orderId, PRODUCT_ID, quantity);
                if (failFirstSend && !sendFailed) {
                    sendFailed = true;
                    throw new IllegalStateException("주입된 결과 전송 실패");
                }
                return new Delivery(1, failures, 0, outcome);
            } catch (RuntimeException e) {
                failures++;
            }
        }
        return new Delivery(0, failures, 1, null); // 재시도 소진: 오류 처리기가 메시지를 건너뛴다
    }

    // ── 방식 3가지 ───────────────────────────────────────────────────────

    /** 기준선: 주문별 기록 없이 재고 숫자만 줄인다 */
    private Approach naive() {
        return new Approach() {
            public String name() {
                return "기록 없이 차감 (기준선)";
            }

            public String process(long orderId, long productId, int quantity) {
                return transactionTemplate.execute(status -> {
                    Stock stock = stockRepository.findByProductIdForUpdate(productId)
                            .orElseThrow(() -> new StockNotFoundException(productId));
                    try {
                        stock.reserve(quantity);
                        return "RESERVED";
                    } catch (InsufficientStockException e) {
                        return "REJECTED";
                    }
                });
            }
        };
    }

    /** 5단계 방식: 예약 기록(order_id UNIQUE)만 있고, 중복이면 UNIQUE 위반 예외 */
    private Approach step5() {
        return new Approach() {
            public String name() {
                return "예약 기록 UNIQUE만 (5단계)";
            }

            public String process(long orderId, long productId, int quantity) {
                try {
                    pessimisticStrategy.reserve(orderId, productId, quantity);
                    return "RESERVED";
                } catch (InsufficientStockException | StockNotFoundException e) {
                    return "REJECTED";
                }
            }
        };
    }

    /** 멱등 처리 (현재 코드, D-033) */
    private Approach idempotent() {
        return new Approach() {
            public String name() {
                return "처리 결과 기록 + 재발행 (6단계)";
            }

            public String process(long orderId, long productId, int quantity) {
                return reservationService.process(orderId, productId, quantity).result().getOutcome() == ReservationOutcome.RESERVED
                        ? "RESERVED" : "REJECTED";
            }
        };
    }

    // ── 측정과 보고 ──────────────────────────────────────────────────────

    record Measure(int reserved, long reservationRecords, int resultsSent, int failures, int gaveUp,
                   String redeliveredOutcome) {
    }

    record Row(String approach, Measure s1, Measure s2, Measure s3) {
    }

    private Measure measure(int resultsSent, int gaveUp, int failures, String redeliveredOutcome) {
        int reserved = stockRepository.findByProductId(PRODUCT_ID).orElseThrow().getReserved();
        long records = reservationRepository.findByOrderId(100L).isPresent() ? 1 : 0;
        return new Measure(reserved, records, resultsSent, failures, gaveUp, redeliveredOutcome);
    }

    private void reset(int onHand) {
        resultRepository.deleteAllInBatch();
        reservationRepository.deleteAllInBatch();
        stockRepository.deleteAllInBatch();
        stockRepository.save(Stock.create(PRODUCT_ID, onHand));
    }

    private static String report(List<Row> rows) {
        StringBuilder sb = new StringBuilder();
        sb.append("## 실험 2 결과 (").append(LocalDate.now()).append(")\n\n");
        sb.append("- 주문 수량 ").append(QUANTITY).append("개. 처리 중 예외가 나면 같은 메시지를 최대 ")
                .append(MAX_ATTEMPTS).append("번 시도한다(Spring Kafka 기본값).\n");
        sb.append("- 재고 차감 = 시나리오 후 stock.reserved. 정상이라면 ①② ").append(QUANTITY).append(", ③ 0.\n\n");
        sb.append("| 방식 | ① 재고 차감 | ① 결과 전송 | ① 재시도 소진 | ② 재고 차감 | ② 결과 전송 | ② 처리 실패 | ② 재시도 소진 | ③ 재전달 시 결정 | ③ 재고 차감 |\n");
        sb.append("|---|---|---|---|---|---|---|---|---|---|\n");
        for (Row r : rows) {
            sb.append("| ").append(r.approach())
                    .append(" | ").append(r.s1().reserved())
                    .append(" | ").append(r.s1().resultsSent())
                    .append(" | ").append(r.s1().gaveUp())
                    .append(" | ").append(r.s2().reserved())
                    .append(" | ").append(r.s2().resultsSent())
                    .append(" | ").append(r.s2().failures())
                    .append(" | ").append(r.s2().gaveUp())
                    .append(" | ").append(r.s3().redeliveredOutcome() == null ? "재시도 소진" : r.s3().redeliveredOutcome())
                    .append(" | ").append(r.s3().reserved())
                    .append(" |\n");
        }
        return sb.toString();
    }
}
