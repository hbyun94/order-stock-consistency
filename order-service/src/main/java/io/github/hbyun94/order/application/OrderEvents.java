package io.github.hbyun94.order.application;

import io.github.hbyun94.order.domain.Order;

/**
 * 주문 이벤트를 내보내는 출구. 구현은 messaging 계층(Outbox)이 맡는다.
 * <p>
 * <b>반드시 주문을 저장하는 트랜잭션 안에서 호출한다.</b> 구현체가 같은 트랜잭션으로 이벤트를 저장해야
 * 주문과 이벤트가 함께 커밋되거나 함께 롤백된다 (D-036).
 */
public interface OrderEvents {

    void orderPlaced(Order order);
}
