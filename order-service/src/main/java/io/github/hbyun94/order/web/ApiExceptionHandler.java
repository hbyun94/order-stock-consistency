package io.github.hbyun94.order.web;

import io.github.hbyun94.order.application.IdempotencyKeyReusedException;
import io.github.hbyun94.order.application.IdempotentRequestInProgressException;
import io.github.hbyun94.order.application.OrderNotFoundException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 * 에러 응답을 RFC 9457 ProblemDetail(application/problem+json) 형식으로 통일한다.
 * ResponseEntityExceptionHandler를 상속해 헤더 누락, 검증 실패 같은 Spring MVC 표준 예외도 같은 형식(400)으로 나간다.
 */
@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(IdempotentRequestInProgressException.class)
    ResponseEntity<ProblemDetail> handleInProgress(IdempotentRequestInProgressException e) {
        // 처리 중인 요청이 곧 끝나므로 잠시 후 같은 키로 재시도하면 결과를 받는다
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .header(HttpHeaders.RETRY_AFTER, "1")
                .body(problem(HttpStatus.CONFLICT, e.getMessage()));
    }

    @ExceptionHandler(IdempotencyKeyReusedException.class)
    ResponseEntity<ProblemDetail> handleKeyReused(IdempotencyKeyReusedException e) {
        return ResponseEntity.unprocessableContent().body(problem(HttpStatus.UNPROCESSABLE_CONTENT, e.getMessage()));
    }

    @ExceptionHandler(OrderNotFoundException.class)
    ResponseEntity<ProblemDetail> handleNotFound(OrderNotFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(problem(HttpStatus.NOT_FOUND, e.getMessage()));
    }

    private static ProblemDetail problem(HttpStatus status, String detail) {
        return ProblemDetail.forStatusAndDetail(status, detail);
    }
}
