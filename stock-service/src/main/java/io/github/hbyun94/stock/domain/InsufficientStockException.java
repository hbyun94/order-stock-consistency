package io.github.hbyun94.stock.domain;

/**
 * 가용 재고가 요청 수량보다 적을 때 발생한다.
 * 버그가 아니라 정상적인 업무 결과이므로, 5단계에서 "재고 부족" 실패 이벤트로 바뀐다.
 */
public class InsufficientStockException extends RuntimeException {

    private final Long productId;
    private final int requested;
    private final int available;

    public InsufficientStockException(Long productId, int requested, int available) {
        super("재고 부족: productId=" + productId + ", 요청=" + requested + ", 가용=" + available);
        this.productId = productId;
        this.requested = requested;
        this.available = available;
    }

    public Long getProductId() {
        return productId;
    }

    public int getRequested() {
        return requested;
    }

    public int getAvailable() {
        return available;
    }
}
