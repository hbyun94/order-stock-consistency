package io.github.hbyun94.order.outbox;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

    /**
     * 미발행 행을 오래된 순으로 잠가서 가져온다 (D-038).
     * <p>
     * {@code FOR UPDATE SKIP LOCKED}: 다른 발행기(다른 인스턴스)가 이미 잠근 행은 기다리지 않고 건너뛴다.
     * 그래서 여러 발행기가 동시에 돌아도 같은 행을 두 번 집어 가지 않고, 서로 다른 행을 나눠 처리한다.
     */
    @Query(value = """
            SELECT * FROM outbox_event
            WHERE published_at IS NULL
            ORDER BY id
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxEvent> lockUnpublished(@Param("limit") int limit);

    long countByPublishedAtIsNull();
}
