package io.github.hbyun94.stock.outbox;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Outbox 폴링 발행기 (D-037).
 * <p>
 * 주기마다: 미발행 행을 잠가서 가져옴(SKIP LOCKED, D-038) → 순서대로 Kafka 전송 → 브로커 확인 후 published_at 기록 → 커밋.
 * <ul>
 *   <li>전송이 실패하면 시도 횟수와 오류를 기록하고 이번 주기를 멈춘다. 다음 주기에 같은 행부터 다시 보낸다.
 *       브로커 장애가 길어져도 행은 지워지지 않으므로 이벤트가 사라지지 않는다.</li>
 *   <li>최소 한 번 발행이다: 전송은 성공했는데 published_at 커밋 전에 죽으면 다음 주기에 다시 보낸다.
 *       이 중복은 소비자의 멱등 처리(D-033, D-034)가 받아낸다.</li>
 *   <li>전송하는 동안 해당 행의 락을 쥐고 있다. 다른 인스턴스의 발행기는 SKIP LOCKED로 그 행을 건너뛴다.</li>
 * </ul>
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxEventRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final TransactionTemplate transactionTemplate;
    private final int batchSize;
    private final long sendTimeoutMillis;

    /**
     * 발행기 전용 KafkaTemplate을 내부에서 만든다. Outbox에는 이미 JSON 문자열로 직렬화된 본문이 있으므로
     * 문자열 그대로 보낸다(기본 KafkaTemplate의 JSON 직렬화기를 쓰면 문자열을 한 번 더 감싼다).
     * 접속 정보와 나머지 설정은 Boot가 만든 ProducerFactory를 물려받는다.
     * 빈으로 등록하지 않는 이유: KafkaTemplate 빈이 하나라도 있으면 Boot가 기본 KafkaTemplate을 만들지 않는다.
     */
    @SuppressWarnings("unchecked")
    public OutboxRelay(
            OutboxEventRepository repository,
            ProducerFactory<?, ?> producerFactory,
            TransactionTemplate transactionTemplate,
            @Value("${outbox.relay.batch-size:100}") int batchSize,
            @Value("${outbox.relay.send-timeout-ms:5000}") long sendTimeoutMillis) {
        this.repository = repository;
        this.kafkaTemplate = new KafkaTemplate<>((ProducerFactory<String, String>) producerFactory
                .copyWithConfigurationOverride(Map.of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)));
        this.transactionTemplate = transactionTemplate;
        this.batchSize = batchSize;
        this.sendTimeoutMillis = sendTimeoutMillis;
    }

    /** 이전 실행이 끝난 뒤 interval만큼 쉬고 다시 실행한다(fixedDelay). 실행이 겹치지 않는다. */
    @Scheduled(fixedDelayString = "${outbox.relay.interval-ms:500}")
    public void relayScheduled() {
        relay();
    }

    /** 한 묶음을 발행하고, 이번에 발행한 건수를 돌려준다. */
    public int relay() {
        Integer published = transactionTemplate.execute(status -> {
            List<OutboxEvent> batch = repository.lockUnpublished(batchSize);
            int count = 0;
            for (OutboxEvent event : batch) {
                if (!send(event)) {
                    break; // 브로커 문제면 뒤의 행도 실패할 가능성이 크므로 이번 주기는 멈춘다
                }
                count++;
            }
            return count;
        });
        return published == null ? 0 : published;
    }

    private boolean send(OutboxEvent event) {
        try {
            kafkaTemplate.send(event.getTopic(), event.getMessageKey(), event.getPayload())
                    .get(sendTimeoutMillis, TimeUnit.MILLISECONDS);
            event.markPublished(); // 변경 감지로 커밋 시 UPDATE
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            event.markFailed(e);
            return false;
        } catch (Exception e) {
            event.markFailed(e);
            log.warn("Outbox 발행 실패, 다음 주기에 다시 시도: id={}, topic={}, attempts={}",
                    event.getId(), event.getTopic(), event.getAttempts(), e);
            return false;
        }
    }
}
