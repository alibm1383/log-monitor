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
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class LogConsumer {

    private final ObjectMapper mapper;
    private final AlertRepository alertRepository;
    private final List<RuleDefinition> rules = new ArrayList<>();
    private final Map<String, Deque<LogEntry>> componentOverallHistory = new ConcurrentHashMap<>();
    private final Map<String, LocalDateTime> lastAlertTime = new ConcurrentHashMap<>();

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

                if (raw.get("coolDown") != null) {
                    rule.setCoolDown((Integer) raw.get("coolDown"));
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
                case OVERALL_LOG_RATE -> checkOverallLogRateRule(rule, entry);
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

    private synchronized void checkOverallLogRateRule(RuleDefinition rule, LogEntry entry) {
        String component = entry.getComponent();
        Deque<LogEntry> history;

        if (componentOverallHistory.containsKey(component)) {
            history = componentOverallHistory.get(component);
        } else {
            history = new ArrayDeque<>();
            componentOverallHistory.put(component, history);
        }

        history.addLast(entry);
        pruneOldEntries(history, entry.getTimestamp(), rule.getWindowSeconds());

        if (history.size() > rule.getThreshold()) {
            String alertKey = rule.getName() + ":" + component;

            if (isCoolDownPassed(alertKey, entry.getTimestamp(), rule.getCoolDown())) {
                double ratePerMinute = (history.size() * 60.0) / rule.getWindowSeconds();
                String description = String.format(
                        "Overall log rate for component %s: %.2f logs/min (%d logs in %d seconds)",
                        component, ratePerMinute, history.size(), rule.getWindowSeconds()
                );
                Alert alert = new Alert
                        (rule.getName(),entry.getComponent(),description, LocalDateTime.now());
                alertRepository.save(alert);
                lastAlertTime.put(alertKey, entry.getTimestamp());
            }
        }
    }

    private void pruneOldEntries(Deque<LogEntry> history , LocalDateTime now , int windowSeconds)
    {
        while (!history.isEmpty()) {
            LogEntry oldest = history.peek();
            long secondsDiff = Duration.between(oldest.getTimestamp(), now).getSeconds();
            if (secondsDiff > windowSeconds) {
                history.pollFirst();
            } else {
                break;
            }
        }
    }

    private boolean isCoolDownPassed(String alertKey , LocalDateTime now , int coolDown)
    {
        LocalDateTime lastAlert = lastAlertTime.get(alertKey);
        if (lastAlert == null)
        {
            return true;
        }
        return Duration.between(lastAlert,now).getSeconds() >= coolDown;
    }
}