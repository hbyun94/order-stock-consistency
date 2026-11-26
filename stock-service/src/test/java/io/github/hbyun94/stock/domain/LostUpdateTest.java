package io.github.hbyun94.stock.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.hbyun94.stock.TestcontainersConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 스레드 없이 두 트랜잭션의 순서를 직접 끼워 넣어 lost update를 결정적으로 재현한다.
 * <pre>
 * 트랜잭션 A: 읽기(reserved=0) ─────────────────────── reserve(1) → 커밋
 * 트랜잭션 B:            읽기(reserved=0) → reserve(1) → 커밋
 * </pre>
 * 같은 순서에서 Stock(version 없음)은 B의 변경을 덮어쓰고, VersionedStock(@Version)은 A가 실패한다.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class LostUpdateTest {

    private static final Long PRODUCT_ID = 1L;

    @Autowired
    StockRepository stockRepository;

    @Autowired
    VersionedStockRepository versionedStockRepository;

    @Autowired
    StockReservationRepository reservationRepository;

    @Autowired
    PlatformTransactionManager transactionManager;

    TransactionTemplate txA;
    TransactionTemplate txB;

    @BeforeEach
    void setUp() {
        reservationRepository.deleteAllInBatch();
        stockRepository.deleteAllInBatch();
        stockRepository.save(Stock.create(PRODUCT_ID, 10));

        txA = new TransactionTemplate(transactionManager);
        // B는 A가 열려 있는 동안 별도 트랜잭션(별도 커넥션)으로 실행하고 먼저 커밋한다
        txB = new TransactionTemplate(transactionManager);
        txB.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    @Test
    @DisplayName("Stock(version 없음): 예약 2번이 모두 성공했지만 reserved는 1이다 = lost update")
    void stockWithoutVersionLosesUpdate() {
        txA.executeWithoutResult(a -> {
            Stock stockA = stockRepository.findByProductId(PRODUCT_ID).orElseThrow(); // reserved=0 읽음

            txB.executeWithoutResult(b ->
                    stockRepository.findByProductId(PRODUCT_ID).orElseThrow().reserve(1)); // reserved=1 커밋

            stockA.reserve(1); // A는 옛 값 0을 기준으로 1을 계산 → 커밋 시 reserved=1로 덮어씀
        });

        assertThat(stockRepository.findByProductId(PRODUCT_ID).orElseThrow().getReserved())
                .as("예약 2건이 성공했는데 1만 반영됨")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("VersionedStock(@Version): 같은 순서에서 늦게 커밋한 A가 실패하고 B의 변경은 보존된다")
    void versionedStockDetectsConflict() {
        assertThatThrownBy(() -> txA.executeWithoutResult(a -> {
            VersionedStock stockA = versionedStockRepository.findByProductId(PRODUCT_ID).orElseThrow(); // version=0

            txB.executeWithoutResult(b ->
                    versionedStockRepository.findByProductId(PRODUCT_ID).orElseThrow().reserve(1)); // version=1 커밋

            stockA.reserve(1); // 커밋 시 UPDATE ... WHERE version = 0 → 0건 갱신 → 예외
        })).isInstanceOf(OptimisticLockingFailureException.class);

        VersionedStock stock = versionedStockRepository.findByProductId(PRODUCT_ID).orElseThrow();
        assertThat(stock.getReserved()).isEqualTo(1);
        assertThat(stock.getVersion()).isEqualTo(1);
    }
}
