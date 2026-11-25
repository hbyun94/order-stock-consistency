package io.github.hbyun94.stock;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * 실제 PostgreSQL 컨테이너(docker-compose와 같은 버전)를 띄워
 * 애플리케이션 컨텍스트가 DB에 연결된 상태로 정상 기동되는지 확인한다.
 */
@SpringBootTest
@Testcontainers
class StockServiceApplicationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18.7");

    @Test
    void contextLoads() {
    }
}
