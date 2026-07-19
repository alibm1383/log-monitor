package org.example.api.controller;

import io.swagger.v3.oas.annotations.Operation;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/alerts")
public class LogController {
    @GetMapping
    public ResponseEntity<String> getAlerts()
    {
        return ResponseEntity.ok("hello");
    }
}
