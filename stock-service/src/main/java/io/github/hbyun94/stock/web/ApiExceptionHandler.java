package io.github.hbyun94.stock.web;

import io.github.hbyun94.stock.application.ReservationNotFoundException;
import io.github.hbyun94.stock.application.StockNotFoundException;
import io.github.hbyun94.stock.domain.ReservationStateException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/** 에러 응답을 RFC 9457 ProblemDetail 형식으로 통일한다 (order-service와 같은 방식) */
@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler({ReservationNotFoundException.class, StockNotFoundException.class})
    ResponseEntity<ProblemDetail> handleNotFound(RuntimeException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(problem(HttpStatus.NOT_FOUND, e.getMessage()));
    }

    /** 상태 전이 위반(이미 확정·해제된 예약). 메시지에 현재 상태가 들어 있다 */
    @ExceptionHandler(ReservationStateException.class)
    ResponseEntity<ProblemDetail> handleConflict(ReservationStateException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(problem(HttpStatus.CONFLICT, e.getMessage()));
    }

    private static ProblemDetail problem(HttpStatus status, String detail) {
        return ProblemDetail.forStatusAndDetail(status, detail);
    }
}
