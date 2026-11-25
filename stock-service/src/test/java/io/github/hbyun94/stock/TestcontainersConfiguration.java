package io.github.hbyun94.stock;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * 테스트용 PostgreSQL 컨테이너 (docker-compose와 같은 버전).
 * <p>
 * 컨테이너를 Spring 빈으로 등록하면, 같은 설정을 쓰는 테스트 클래스들이
 * 캐시된 애플리케이션 컨텍스트와 함께 컨테이너 하나를 공유한다.
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

    @Bean
    @ServiceConnection
    PostgreSQLContainer postgresContainer() {
        return new PostgreSQLContainer("postgres:18.7");
    }
}
