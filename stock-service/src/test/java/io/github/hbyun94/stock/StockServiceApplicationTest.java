package io.github.hbyun94.stock;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

/**
 * 실제 PostgreSQL 컨테이너를 띄워 애플리케이션 컨텍스트가 정상 기동되는지 확인한다.
 * Flyway 마이그레이션 적용과 Hibernate 스키마 검증(ddl-auto: validate)도 이때 함께 검증된다.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class StockServiceApplicationTest {

    @Test
    void contextLoads() {
    }
}
