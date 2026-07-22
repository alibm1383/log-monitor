package org.example.api.controller;

import io.swagger.v3.oas.annotations.Operation;
import org.example.api.entity.Alert;
import org.example.api.repository.AlertRepository;
import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/alerts")
public class LogController {
    private final AlertRepository alertRepository;
    public LogController(AlertRepository alertRepository) {
        this.alertRepository = alertRepository;
    }
    @GetMapping
    public ResponseEntity<List<Alert>> getAlerts()
    {
        return ResponseEntity.ok(alertRepository.findAll(Sort.by(Sort.Direction.DESC,"CreatedAt")));
    }
}
