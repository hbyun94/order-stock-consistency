package io.github.hbyun94.stock.application;

public class StockNotFoundException extends RuntimeException {

    public StockNotFoundException(Long productId) {
        super("재고가 등록되지 않은 상품입니다: productId=" + productId);
    }
}
