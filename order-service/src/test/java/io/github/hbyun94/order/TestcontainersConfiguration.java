package io.github.hbyun94.order;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * 테스트용 PostgreSQL, Redis 컨테이너 (docker-compose와 같은 버전).
 * 같은 설정을 쓰는 테스트 클래스들이 캐시된 컨텍스트와 함께 컨테이너를 공유한다.
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

    @Bean
    @ServiceConnection
    PostgreSQLContainer postgresContainer() {
        return new PostgreSQLContainer("postgres:18.7");
    }

    @Bean
    @ServiceConnection(name = "redis")
    GenericContainer<?> redisContainer() {
        return new GenericContainer<>("redis:8.10.3").withExposedPorts(6379);
    }
}
