package io.github.hbyun94.stock;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * 테스트용 PostgreSQL, Redis, Kafka 컨테이너 (docker-compose와 같은 버전).
 * <p>
 * 컨테이너를 Spring 빈으로 등록하면, 같은 설정을 쓰는 테스트 클래스들이
 * 캐시된 애플리케이션 컨텍스트와 함께 컨테이너를 공유한다.
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

    @Bean
    @ServiceConnection
    KafkaContainer kafkaContainer() {
        // KRaft 단일 노드. docker-compose와 같은 브로커 버전
        return new KafkaContainer("apache/kafka:4.3.2");
    }
}
