package io.github.hbyun94.order.messaging;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJacksonJsonMessageConverter;

@Configuration
public class KafkaConfig {

    // 토픽을 코드로 선언한다 (D-031). 기동 시 KafkaAdmin이 없으면 만들고, 있으면 그대로 둔다.
    // 파티션 3: 소비자 인스턴스를 최대 3개까지 병렬로 붙일 수 있다. 복제 1: 로컬 브로커가 1대다.
    private static final int PARTITIONS = 3;
    private static final int REPLICAS = 1;

    @Bean
    NewTopic orderCreatedTopic() {
        return TopicBuilder.name(KafkaTopics.ORDER_CREATED).partitions(PARTITIONS).replicas(REPLICAS).build();
    }

    @Bean
    NewTopic stockReservedTopic() {
        return TopicBuilder.name(KafkaTopics.STOCK_RESERVED).partitions(PARTITIONS).replicas(REPLICAS).build();
    }

    @Bean
    NewTopic stockReservationFailedTopic() {
        return TopicBuilder.name(KafkaTopics.STOCK_RESERVATION_FAILED).partitions(PARTITIONS).replicas(REPLICAS).build();
    }

    /** 소비하는 토픽의 DLT. 원래 파티션 번호로 보내므로 파티션 수를 원래 토픽과 같게 둔다 (D-045) */
    @Bean
    NewTopic stockReservedDeadLetterTopic() {
        return TopicBuilder.name(KafkaTopics.STOCK_RESERVED_DLT).partitions(PARTITIONS).replicas(REPLICAS).build();
    }

    @Bean
    NewTopic stockReservationFailedDeadLetterTopic() {
        return TopicBuilder.name(KafkaTopics.STOCK_RESERVATION_FAILED_DLT).partitions(PARTITIONS).replicas(REPLICAS).build();
    }

    /**
     * 문자열로 받은 JSON을 @KafkaListener 메서드의 파라미터 타입(record)으로 변환한다.
     * Spring Boot가 이 빈을 기본 리스너 컨테이너 팩토리에 연결한다.
     * 타입 헤더 없이도 파라미터 타입만 보고 변환하므로, 보내는 쪽의 클래스 이름을 몰라도 된다.
     */
    @Bean
    RecordMessageConverter messageConverter() {
        return new StringJacksonJsonMessageConverter();
    }
}
