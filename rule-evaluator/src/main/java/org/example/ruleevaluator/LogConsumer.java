package org.example.ruleevaluator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.annotation.PostConstruct;
import org.example.ruleevaluator.model.Alert;
import org.example.ruleevaluator.model.LogEntry;
import org.example.ruleevaluator.model.RuleDefinition;
import org.example.ruleevaluator.model.RuleType;
import org.example.ruleevaluator.repository.AlertRepository;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class LogConsumer {

    private final ObjectMapper mapper;
    private final AlertRepository alertRepository;
    private List<RuleDefinition> rules = new ArrayList<>();

    public LogConsumer(AlertRepository alertRepository) {
        this.mapper = new ObjectMapper();
        this.mapper.registerModule(new JavaTimeModule());
        this.alertRepository = alertRepository;
    }

    @KafkaListener(topics = "raw-logs", groupId = "rule-evaluator-group")
    public void consume(String message) {
        try {
            LogEntry entry = mapper.readValue(message, LogEntry.class);
            evaluateRules(entry);
            System.out.println(message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @PostConstruct
    public void loadRules() {
        Yaml yaml = new Yaml();

        try (InputStream in = new ClassPathResource("rules.yml").getInputStream()) {

            Map<String, Object> data = yaml.load(in);
            List<Map<String, Object>> rawRules = (List<Map<String, Object>>) data.get("rules");

            for (var raw : rawRules) {
                RuleDefinition rule = new RuleDefinition();

                rule.setName((String) raw.get("name"));
                rule.setType(RuleType.valueOf((String) raw.get("type")));

                if (raw.get("level") != null) {
                    rule.setLevel((String) raw.get("level"));
                }
                if (raw.get("windowSeconds") != null) {
                    rule.setWindowSeconds((Integer) raw.get("windowSeconds"));
                }
                if (raw.get("threshold") != null) {
                    rule.setThreshold((Integer) raw.get("threshold"));
                }

                rules.add(rule);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void evaluateRules(LogEntry entry) {
        for (RuleDefinition rule : rules) {
            switch (rule.getType()) {
                case LOG_LEVEL -> checkLogLevel(rule, entry);
                //case COMPONENT_LOG_RATE -> checkComponentLogRateRule(rule, entry);
                //case OVERALL_LOG_RATE -> checkOverallLogRateRule(rule, entry);
            }
        }
    }

    void checkLogLevel(RuleDefinition rule ,LogEntry log)
    {
        if (log.getLevel() != null && log.getLevel().equalsIgnoreCase(rule.getLevel()))
        {
            Alert alert = new Alert
                    (rule.getName(),log.getComponent(),log.getMessage(), LocalDateTime.now());
            alertRepository.save(alert);
        }
    }

}