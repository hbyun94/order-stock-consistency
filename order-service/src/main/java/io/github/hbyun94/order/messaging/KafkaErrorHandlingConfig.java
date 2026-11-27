package io.github.hbyun94.order.messaging;

import io.github.hbyun94.order.application.OrderNotFoundException;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

/**
 * 리스너 처리 실패 시의 재시도와 DLT 정책 (D-042 ~ D-046).
 * <ul>
 *   <li>같은 자리에서 재시도한다(블로킹). 메시지 키(주문 ID)별 처리 순서가 유지된다 (D-042).</li>
 *   <li>간격은 지수 백오프(기본 1s → 2s → 4s → 8s, 재시도 4회)다 (D-043).</li>
 *   <li>다시 해도 결과가 같은 실패(깨진 JSON, 필수 값 누락, 없는 주문)는 재시도하지 않고 바로 DLT로 보낸다 (D-044).</li>
 *   <li>재시도를 다 쓰면 {@code <원래 토픽>.dlt}의 같은 파티션으로 원문을 보낸다. 실패 원인은 헤더에 남는다 (D-045).</li>
 *   <li>DLT 전송까지 실패하면 오프셋을 커밋하지 않고 같은 메시지를 다시 처리한다. 메시지는 사라지지 않는다.</li>
 * </ul>
 * Spring Boot는 CommonErrorHandler 빈을 기본 리스너 컨테이너 팩토리에 연결한다.
 */
@Configuration
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    @Bean
    CommonErrorHandler kafkaErrorHandler(
            ProducerFactory<?, ?> producerFactory,
            @Value("${kafka.retry.initial-interval-ms:1000}") long initialIntervalMs,
            @Value("${kafka.retry.multiplier:2.0}") double multiplier,
            @Value("${kafka.retry.max-interval-ms:8000}") long maxIntervalMs,
            @Value("${kafka.retry.max-retries:4}") int maxRetries) {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                deadLetterTemplate(producerFactory),
                (record, ex) -> new TopicPartition(KafkaTopics.deadLetterOf(record.topic()), record.partition()));

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(maxRetries);
        backOff.setInitialInterval(initialIntervalMs);
        backOff.setMultiplier(multiplier);
        backOff.setMaxInterval(maxIntervalMs);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        // 깨진 JSON(메시지 변환 실패)은 기본 설정으로 이미 재시도 대상이 아니다. 필수 값 누락과 없는 주문을 추가한다.
        // 없는 주문: 주문은 이벤트보다 먼저 커밋되므로(Outbox, D-036) 기다려도 생기지 않는다
        handler.addNotRetryableExceptions(InvalidEventException.class, OrderNotFoundException.class);
        handler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
                log.warn("메시지 처리 실패 (시도 {}회째): topic={}, partition={}, offset={}, key={}, error={}",
                        deliveryAttempt, record.topic(), record.partition(), record.offset(), record.key(),
                        rootMessage(ex));
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
                // 사람이 확인해야 하는 지점이므로 ERROR로 남긴다 (D-046: DLT 소비자는 두지 않는다)
                log.error("메시지를 DLT로 보냄: {} ← topic={}, partition={}, offset={}, key={}, error={}",
                        KafkaTopics.deadLetterOf(record.topic()), record.topic(), record.partition(),
                        record.offset(), record.key(), rootMessage(ex));
            }
        });
        return handler;
    }

    /**
     * DLT 전송용 KafkaTemplate. 리스너가 받은 값은 문자열(원문 JSON)이므로 문자열 그대로 보낸다.
     * 기본 KafkaTemplate의 JSON 직렬화기를 쓰면 문자열을 한 번 더 따옴표로 감싼다.
     * 빈으로 등록하지 않는 이유는 OutboxRelay와 같다: KafkaTemplate 빈이 있으면 Boot가 기본 템플릿을 만들지 않는다.
     */
    @SuppressWarnings("unchecked")
    private static KafkaTemplate<String, String> deadLetterTemplate(ProducerFactory<?, ?> producerFactory) {
        return new KafkaTemplate<>((ProducerFactory<String, String>) producerFactory.copyWithConfigurationOverride(
                Map.of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)));
    }

    private static String rootMessage(Throwable ex) {
        Throwable root = ex;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getClass().getSimpleName() + ": " + root.getMessage();
    }
}
