package io.github.hbyun94.order.web;

import static org.assertj.core.api.Assertions.assertThat;

import com.jayway.jsonpath.JsonPath;
import io.github.hbyun94.order.TestcontainersConfiguration;
import io.github.hbyun94.order.domain.OrderRepository;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

/** HTTP 계층: 상태 코드, 헤더, ProblemDetail 응답 형식을 검증한다 (실제 PostgreSQL + Redis) */
@SpringBootTest
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
class OrderControllerTest {

    private static final String BODY = """
            {"productId": 1, "quantity": 2}
            """;

    @Autowired
    MockMvcTester mvc;

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
    @DisplayName("POST /orders: 201 Created, Location 헤더, PENDING 주문")
    void placeOrder() throws Exception {
        MvcTestResult result = post(key, BODY);

        assertThat(result).hasStatus(HttpStatus.CREATED)
                .hasHeader("Idempotent-Replayed", "false")
                .bodyJson().extractingPath("$.status").isEqualTo("PENDING");
        Number orderId = JsonPath.read(result.getResponse().getContentAsString(), "$.orderId");
        assertThat(result.getResponse().getHeader("Location")).isEqualTo("/orders/" + orderId);
    }

    @Test
    @DisplayName("같은 키로 재요청하면 같은 201 응답(같은 주문 ID)과 Idempotent-Replayed: true")
    void replay() throws Exception {
        String first = post(key, BODY).getResponse().getContentAsString();

        MvcTestResult second = post(key, BODY);

        assertThat(second).hasStatus(HttpStatus.CREATED).hasHeader("Idempotent-Replayed", "true");
        assertThat(second.getResponse().getContentAsString()).isEqualTo(first);
        assertThat(orderRepository.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("같은 키로 다른 내용이면 422 ProblemDetail")
    void sameKeyDifferentBody() {
        post(key, BODY);

        MvcTestResult result = post(key, """
                {"productId": 1, "quantity": 3}
                """);

        assertThat(result).hasStatus(HttpStatus.UNPROCESSABLE_CONTENT)
                .hasContentType(MediaType.APPLICATION_PROBLEM_JSON)
                .bodyJson().extractingPath("$.status").isEqualTo(422);
    }

    @Test
    @DisplayName("같은 키의 요청이 처리 중이면 409와 Retry-After 헤더")
    void inProgress() throws Exception {
        // 다른 요청이 이 키를 선점해 처리 중인 상황을 Redis에 직접 만든다
        redis.opsForValue().set("idempotency:order:" + key, "PROCESSING:" + sha256("productId=1;quantity=2"));

        MvcTestResult result = post(key, BODY);

        assertThat(result).hasStatus(HttpStatus.CONFLICT).hasHeader("Retry-After", "1");
        assertThat(orderRepository.count()).isZero();
    }

    @Test
    @DisplayName("Idempotency-Key 헤더가 없으면 400 ProblemDetail")
    void missingIdempotencyKey() {
        MvcTestResult result = mvc.post().uri("/orders").contentType(MediaType.APPLICATION_JSON).content(BODY).exchange();

        assertThat(result).hasStatus(HttpStatus.BAD_REQUEST).hasContentType(MediaType.APPLICATION_PROBLEM_JSON);
    }

    @Test
    @DisplayName("수량이 0이면 400 ProblemDetail")
    void invalidQuantity() {
        MvcTestResult result = post(key, """
                {"productId": 1, "quantity": 0}
                """);

        assertThat(result).hasStatus(HttpStatus.BAD_REQUEST).hasContentType(MediaType.APPLICATION_PROBLEM_JSON);
        assertThat(orderRepository.count()).isZero();
    }

    @Test
    @DisplayName("GET /orders/{id}: 있으면 200, 없으면 404 ProblemDetail")
    void getOrder() throws Exception {
        String created = post(key, BODY).getResponse().getContentAsString();
        Number orderId = JsonPath.read(created, "$.orderId");

        assertThat(mvc.get().uri("/orders/{id}", orderId).exchange())
                .hasStatusOk().bodyJson().extractingPath("$.productId").isEqualTo(1);
        assertThat(mvc.get().uri("/orders/{id}", 999_999).exchange())
                .hasStatus(HttpStatus.NOT_FOUND).hasContentType(MediaType.APPLICATION_PROBLEM_JSON);
    }

    private MvcTestResult post(String idempotencyKey, String body) {
        return mvc.post().uri("/orders")
                .header("Idempotency-Key", idempotencyKey)
                .contentType(MediaType.APPLICATION_JSON)
                .content(body)
                .exchange();
    }

    private static String sha256(String value) throws Exception {
        byte[] hash = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
        return HexFormat.of().formatHex(hash);
    }
}
