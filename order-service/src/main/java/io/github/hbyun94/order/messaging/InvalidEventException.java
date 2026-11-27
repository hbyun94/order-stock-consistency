package io.github.hbyun94.order.messaging;

/**
 * 형식은 맞지만 필수 값이 빠졌거나 잘못된 이벤트. 다시 처리해도 결과가 같으므로 재시도하지 않고 바로 DLT로 보낸다 (D-044).
 */
public class InvalidEventException extends RuntimeException {

    public InvalidEventException(String message) {
        super(message);
    }
}
