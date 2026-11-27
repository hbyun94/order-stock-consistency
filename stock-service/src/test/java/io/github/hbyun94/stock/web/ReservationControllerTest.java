package io.github.hbyun94.stock.web;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.hbyun94.stock.TestcontainersConfiguration;
import io.github.hbyun94.stock.application.StockService;
import io.github.hbyun94.stock.domain.Stock;
import io.github.hbyun94.stock.domain.StockRepository;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

/** 출고 확정·예약 해제 API: 상태 코드, 응답 본문, 재고 변화, ProblemDetail 형식을 검증한다 (실제 PostgreSQL) */
@SpringBootTest
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
class ReservationControllerTest {

    @Autowired
    MockMvcTester mvc;

    @Autowired
    StockService stockService;

    @Autowired
    StockRepository stockRepository;

    long productId;
    long orderId;

    @BeforeEach
    void setUp() {
        productId = ThreadLocalRandom.current().nextLong(1, 1_000_000);
        orderId = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
        stockRepository.save(Stock.create(productId, 10));
        stockService.reserve(orderId, productId, 3);
    }

    @Test
    @DisplayName("POST /reservations/{orderId}/confirm: 200, CONFIRMED, 실물 재고와 예약 재고가 함께 줄어든다")
    void confirm() {
        MvcTestResult result = post("/reservations/" + orderId + "/confirm");

        assertThat(result).hasStatus(HttpStatus.OK)
                .bodyJson().extractingPath("$.status").isEqualTo("CONFIRMED");
        assertThat(result).bodyJson().extractingPath("$.quantity").isEqualTo(3);
        Stock stock = stockRepository.findByProductId(productId).orElseThrow();
        assertThat(stock.getOnHand()).isEqualTo(7);
        assertThat(stock.getReserved()).isZero();
    }

    @Test
    @DisplayName("POST /reservations/{orderId}/release: 200, RELEASED, 예약 재고만 줄어 가용 재고가 돌아온다")
    void release() {
        MvcTestResult result = post("/reservations/" + orderId + "/release");

        assertThat(result).hasStatus(HttpStatus.OK)
                .bodyJson().extractingPath("$.status").isEqualTo("RELEASED");
        Stock stock = stockRepository.findByProductId(productId).orElseThrow();
        assertThat(stock.getOnHand()).isEqualTo(10);
        assertThat(stock.getReserved()).isZero();
    }

    @Test
    @DisplayName("이미 확정된 예약을 다시 확정하거나 해제하면 409 ProblemDetail이고, 재고는 그대로다")
    void conflictAfterConfirm() {
        assertThat(post("/reservations/" + orderId + "/confirm")).hasStatus(HttpStatus.OK);

        MvcTestResult again = post("/reservations/" + orderId + "/confirm");
        assertThat(again).hasStatus(HttpStatus.CONFLICT)
                .hasContentType(MediaType.APPLICATION_PROBLEM_JSON)
                .bodyJson().extractingPath("$.detail").asString().contains("현재 상태=CONFIRMED");
        assertThat(post("/reservations/" + orderId + "/release")).hasStatus(HttpStatus.CONFLICT);

        Stock stock = stockRepository.findByProductId(productId).orElseThrow();
        assertThat(stock.getOnHand()).isEqualTo(7); // 두 번째 확정이 재고를 또 줄이지 않았다
        assertThat(stock.getReserved()).isZero();
    }

    @Test
    @DisplayName("예약이 없는 주문이면 404 ProblemDetail")
    void notFound() {
        long unknownOrderId = orderId == Long.MAX_VALUE ? 1 : orderId + 1;

        assertThat(post("/reservations/" + unknownOrderId + "/confirm"))
                .hasStatus(HttpStatus.NOT_FOUND)
                .hasContentType(MediaType.APPLICATION_PROBLEM_JSON)
                .bodyJson().extractingPath("$.status").isEqualTo(404);
    }

    private MvcTestResult post(String uri) {
        return mvc.post().uri(uri).exchange();
    }
}
