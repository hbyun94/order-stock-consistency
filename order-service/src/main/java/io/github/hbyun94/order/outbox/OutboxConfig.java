package io.github.hbyun94.order.outbox;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Outbox 발행기(@Scheduled)를 돌리기 위한 스케줄링 활성화 */
@Configuration
@EnableScheduling
public class OutboxConfig {
}
