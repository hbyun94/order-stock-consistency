package io.github.hbyun94.stock.application;

import io.github.hbyun94.stock.domain.InsufficientStockException;
import io.github.hbyun94.stock.domain.OrderEventResult;
import io.github.hbyun94.stock.domain.OrderEventResultRepository;
import io.github.hbyun94.stock.domain.RejectionReason;
import io.github.hbyun94.stock.domain.Stock;
import io.github.hbyun94.stock.domain.StockRepository;
import io.github.hbyun94.stock.domain.StockReservation;
import io.github.hbyun94.stock.domain.StockReservationRepository;
import java.util.Optional;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * order.created 한 건을 멱등하게 처리한다 (D-033).
 * <p>
 * 주문 하나에 대한 결정(예약 성공/거절)은 딱 한 번만 내리고 {@link OrderEventResult}에 기록한다.
 * 같은 주문이 다시 오면 새로 판단하지 않고 기록된 결정을 돌려준다. 호출자는 그 결정으로 같은 결과 이벤트를 다시 보낸다.
 * <p>
 * 중복을 막는 장치는 세 겹이다.
 * <ol>
 *   <li>처리 전 조회: 이미 기록이 있으면 바로 돌려준다 (대부분의 재전달).</li>
 *   <li>락을 얻은 뒤 다시 조회: 같은 상품의 처리는 재고 행 락(D-024)으로 줄을 서므로,
 *       동시에 들어온 같은 주문의 두 번째 처리는 앞선 처리가 커밋한 기록을 여기서 본다.</li>
 *   <li>PK(order_id) 제약: 위를 모두 통과한 경우(예: 미등록 상품이라 잠글 행이 없을 때)에도
 *       두 번째 INSERT는 DB가 막는다. 진 쪽은 롤백하고 이긴 쪽의 기록을 읽는다.</li>
 * </ol>
 * 결정과 기록, 재고 변경은 한 트랜잭션이다. 그래서 "재고는 바뀌었는데 기록이 없는" 상태가 생기지 않는다.
 */
@Service
public class OrderReservationService {

    private final StockRepository stockRepository;
    private final StockReservationRepository reservationRepository;
    private final OrderEventResultRepository resultRepository;
    private final TransactionTemplate transactionTemplate;

    public OrderReservationService(
            StockRepository stockRepository,
            StockReservationRepository reservationRepository,
            OrderEventResultRepository resultRepository,
            TransactionTemplate transactionTemplate) {
        this.stockRepository = stockRepository;
        this.reservationRepository = reservationRepository;
        this.resultRepository = resultRepository;
        this.transactionTemplate = transactionTemplate;
    }

    /** 처리 결과와, 이번 호출이 중복(이미 결정된 주문)이었는지 */
    public record Processed(OrderEventResult result, boolean duplicate) {
    }

    public Processed process(Long orderId, Long productId, int quantity) {
        Optional<OrderEventResult> recorded = resultRepository.findById(orderId);
        if (recorded.isPresent()) {
            return new Processed(recorded.get(), true);
        }
        try {
            return transactionTemplate.execute(status -> decide(orderId, productId, quantity));
        } catch (DataIntegrityViolationException e) {
            // 같은 주문을 동시에 처리한 쪽이 먼저 기록했다. 이쪽 트랜잭션(재고 변경 포함)은 통째로 롤백됐다.
            OrderEventResult winner = resultRepository.findById(orderId).orElseThrow(() -> e);
            return new Processed(winner, true);
        }
    }

    private Processed decide(Long orderId, Long productId, int quantity) {
        Optional<Stock> stock = stockRepository.findByProductIdForUpdate(productId); // 비관적 락 (D-024)

        Optional<OrderEventResult> recordedWhileWaiting = resultRepository.findById(orderId);
        if (recordedWhileWaiting.isPresent()) {
            return new Processed(recordedWhileWaiting.get(), true);
        }

        if (stock.isEmpty()) {
            return record(OrderEventResult.rejected(orderId, productId, quantity, RejectionReason.UNKNOWN_PRODUCT));
        }
        try {
            // 도메인 메서드라 트랜잭션 프록시를 거치지 않는다: 예외가 나도 트랜잭션이 rollback-only가 되지 않는다
            stock.get().reserve(quantity);
        } catch (InsufficientStockException e) {
            return record(OrderEventResult.rejected(orderId, productId, quantity, RejectionReason.INSUFFICIENT_STOCK));
        }
        reservationRepository.save(StockReservation.reserve(orderId, productId, quantity));
        return record(OrderEventResult.reserved(orderId, productId, quantity));
    }

    private Processed record(OrderEventResult result) {
        // saveAndFlush: PK 위반을 커밋 시점이 아니라 여기서 바로 드러내 트랜잭션 안에서 실패하게 한다
        return new Processed(resultRepository.saveAndFlush(result), false);
    }
}
