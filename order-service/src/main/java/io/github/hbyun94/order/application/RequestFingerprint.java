package io.github.hbyun94.order.application;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * 요청 내용의 지문(SHA-256 16진수 64자).
 * 같은 Idempotency-Key로 내용이 다른 요청이 오면 지문이 달라져서 거절할 수 있다 (D-020).
 */
final class RequestFingerprint {

    private RequestFingerprint() {
    }

    static String of(PlaceOrderCommand command) {
        // 필드 순서와 구분자를 고정해 같은 내용이면 항상 같은 문자열이 되게 한다
        String canonical = "productId=" + command.productId() + ";quantity=" + command.quantity();
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256을 사용할 수 없습니다", e);
        }
    }
}
