package io.github.hbyun94.stock.web;

import io.github.hbyun94.stock.application.StockService;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 출고 확정과 예약 해제 API (D-048).
 * <p>
 * 출고를 마친 창고 시스템(WMS)이 호출하는 자리라고 본다. 예약(order.created)과 확정을 나눈 설계(D-006)를
 * 실제로 끝까지 돌려 볼 수 있게 하는 최소 진입점이다. 확정 결과를 order-service로 알리지는 않는다(범위 밖).
 * <ul>
 *   <li>예약이 없으면 404</li>
 *   <li>이미 확정·해제된 예약이면 409. 응답 본문에 현재 상태를 담는다</li>
 * </ul>
 */
@RestController
@RequestMapping("/reservations")
public class ReservationController {

    private final StockService stockService;

    public ReservationController(StockService stockService) {
        this.stockService = stockService;
    }

    /** 출고 확정: 예약 수량만큼 실물 재고(onHand)와 예약 재고(reserved)를 함께 줄인다 */
    @PostMapping("/{orderId}/confirm")
    public ReservationResponse confirm(@PathVariable Long orderId) {
        return ReservationResponse.from(stockService.confirm(orderId));
    }

    /** 예약 해제: 예약 재고만 줄여 가용 재고를 되돌린다 */
    @PostMapping("/{orderId}/release")
    public ReservationResponse release(@PathVariable Long orderId) {
        return ReservationResponse.from(stockService.release(orderId));
    }
}
