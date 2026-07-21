package org.example.ruleevaluator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.annotation.PostConstruct;
import org.example.ruleevaluator.model.LogEntry;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class LogConsumer {

    private final ObjectMapper mapper;

    public LogConsumer() {
        this.mapper = new ObjectMapper();
        this.mapper.registerModule(new JavaTimeModule());
    }

    @KafkaListener(topics = "raw-logs", groupId = "rule-evaluator-group")
    public void consume(String message) {
        try {
            LogEntry entry = mapper.readValue(message, LogEntry.class);
            System.out.println(message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}