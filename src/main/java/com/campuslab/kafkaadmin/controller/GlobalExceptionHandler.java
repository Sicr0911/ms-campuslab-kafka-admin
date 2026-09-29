package com.campuslab.kafkaadmin.controller;

import com.campuslab.kafkaadmin.service.KafkaAdminException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(KafkaAdminException.class)
    public ResponseEntity<Map<String, String>> handleKafkaUnavailable(KafkaAdminException ex) {
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(Map.of("message", ex.getMessage()));
    }
}
