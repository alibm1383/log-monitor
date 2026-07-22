package org.example.ruleevaluator;

import org.example.ruleevaluator.model.LogEntry;
import org.example.ruleevaluator.model.RuleDefinition;
import org.example.ruleevaluator.model.RuleType;
import org.example.ruleevaluator.repository.AlertRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.Deque;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class LogConsumerTest {

    private AlertRepository alertRepository;
    private KafkaTemplate<String, String> kafkaTemplate;
    private LogConsumer consumer;
    private final LocalDateTime base = LocalDateTime.of(2026, 7, 22, 10, 0, 0);

    @BeforeEach
    void setUp() {
        alertRepository = mock(AlertRepository.class);
        kafkaTemplate = mock(KafkaTemplate.class);
        consumer = new LogConsumer(alertRepository,kafkaTemplate);
    }

    private LogEntry entry(String component, String level, LocalDateTime timestamp, String message) {
        return new LogEntry(component, timestamp, "main", level, "com.example.Foo", message);
    }


    @Nested
    @DisplayName("pruneOldEntries")
    class PruneOldEntriesTests {

        @Test
        @DisplayName("removes entries older than the window")
        void removesEntriesOlderThanWindow() {
            Deque<LogEntry> history = new ArrayDeque<>();
            history.addLast(entry("c", "INFO", base, "msg1"));
            history.addLast(entry("c", "INFO", base.plusSeconds(30), "msg2"));
            history.addLast(entry("c", "INFO", base.plusSeconds(130), "msg3"));

            consumer.pruneOldEntries(history, base.plusSeconds(130), 60);

            assertEquals(1, history.size());
            assertEquals("msg3", history.peekFirst().getMessage());
        }

        @Test
        @DisplayName("keeps all entries within the window")
        void keepsAllEntriesWithinWindow() {
            Deque<LogEntry> history = new ArrayDeque<>();
            history.addLast(entry("c", "INFO", base, "msg1"));
            history.addLast(entry("c", "INFO", base.plusSeconds(10), "msg2"));

            consumer.pruneOldEntries(history, base.plusSeconds(10), 60);

            assertEquals(2, history.size());
        }

        @Test
        @DisplayName("does nothing on an empty deque")
        void doesNothingOnEmptyDeque() {
            Deque<LogEntry> history = new ArrayDeque<>();
            assertDoesNotThrow(() -> consumer.pruneOldEntries(history, base, 60));
            assertTrue(history.isEmpty());
        }

        @Test
        @DisplayName("removes all entries if all are too old")
        void removesAllIfAllTooOld() {
            Deque<LogEntry> history = new ArrayDeque<>();
            history.addLast(entry("c", "INFO", base, "msg1"));
            history.addLast(entry("c", "INFO", base.plusSeconds(5), "msg2"));

            consumer.pruneOldEntries(history, base.plusSeconds(1000), 60);

            assertTrue(history.isEmpty());
        }
    }


    @Nested
    @DisplayName("isCoolDownPassed")
    class IsCoolDownPassedTests {

        @Test
        @DisplayName("returns true when there was no previous alert")
        void trueWhenNoPreviousAlert() {
            assertTrue(consumer.isCoolDownPassed("some-key", base, 60));
        }

        @Test
        @DisplayName("returns false while still within cooldown")
        void falseWithinCooldown() {
            RuleDefinition rule = rateRule("overall-test", RuleType.OVERALL_LOG_RATE, null, 60, 1, 60);
            consumer.checkOverallLogRate(rule, entry("c", "INFO", base, "m1"));
            consumer.checkOverallLogRate(rule, entry("c", "INFO", base.plusSeconds(5), "m2"));

            String key = "overall-test|c";
            boolean passed = consumer.isCoolDownPassed(key, base.plusSeconds(30), 60);

            assertFalse(passed);
        }
    }


    @Nested
    @DisplayName("getLastTwoMessages")
    class GetLastTwoMessagesTests {

        @Test
        @DisplayName("returns both messages in reverse order when two entries exist")
        void twoEntries() {
            Deque<LogEntry> history = new ArrayDeque<>();
            history.addLast(entry("c", "INFO", base, "first"));
            history.addLast(entry("c", "INFO", base.plusSeconds(10), "second"));

            assertEquals("second\nfirst", consumer.getLastTwoMessages(history));
        }

        @Test
        @DisplayName("returns only the most recent message when only one entry exists")
        void oneEntry() {
            Deque<LogEntry> history = new ArrayDeque<>();
            history.addLast(entry("c", "INFO", base, "only"));

            assertEquals("only\n", consumer.getLastTwoMessages(history));
        }

        @Test
        @DisplayName("returns only the last two messages when more than two exist")
        void moreThanTwoEntries() {
            Deque<LogEntry> history = new ArrayDeque<>();
            history.addLast(entry("c", "INFO", base, "first"));
            history.addLast(entry("c", "INFO", base.plusSeconds(10), "second"));
            history.addLast(entry("c", "INFO", base.plusSeconds(20), "third"));

            assertEquals("third\nsecond", consumer.getLastTwoMessages(history));
        }
    }


    @Nested
    @DisplayName("checkLogLevel")
    class CheckLogLevelTests {

        @Test
        @DisplayName("saves an alert when the log level matches the rule")
        void savesAlertWhenLevelMatches() {
            RuleDefinition rule = new RuleDefinition();
            rule.setName("error-alert");
            rule.setType(RuleType.LOG_LEVEL);
            rule.setLevel("ERROR");

            consumer.checkLogLevel(rule, entry("orders", "ERROR", base, "something broke"));

            verify(alertRepository, times(1)).save(any());
        }

        @Test
        @DisplayName("does not save an alert when the log level differs")
        void doesNotSaveWhenLevelDiffers() {
            RuleDefinition rule = new RuleDefinition();
            rule.setName("error-alert");
            rule.setType(RuleType.LOG_LEVEL);
            rule.setLevel("ERROR");

            consumer.checkLogLevel(rule, entry("orders", "INFO", base, "all good"));

            verify(alertRepository, never()).save(any());
        }
    }


    @Nested
    @DisplayName("checkComponentLogRate")
    class CheckComponentLogRateTests {

        @Test
        @DisplayName("saves an alert once the threshold is exceeded")
        void savesAlertWhenThresholdExceeded() {
            RuleDefinition rule = rateRule("component-error-rate", RuleType.COMPONENT_LOG_RATE, "ERROR", 60, 2, 60);

            consumer.checkComponentLogRate(rule, entry("orders", "ERROR", base, "e1"));
            consumer.checkComponentLogRate(rule, entry("orders", "ERROR", base.plusSeconds(10), "e2"));
            consumer.checkComponentLogRate(rule, entry("orders", "ERROR", base.plusSeconds(20), "e3"));

            verify(alertRepository, times(1)).save(any());
        }

        @Test
        @DisplayName("does not save an alert below the threshold")
        void doesNotSaveBelowThreshold() {
            RuleDefinition rule = rateRule("component-error-rate", RuleType.COMPONENT_LOG_RATE, "ERROR", 60, 2, 60);

            consumer.checkComponentLogRate(rule, entry("orders", "ERROR", base, "e1"));
            consumer.checkComponentLogRate(rule, entry("orders", "ERROR", base.plusSeconds(10), "e2"));

            verify(alertRepository, never()).save(any());
        }

        @Test
        @DisplayName("ignores logs with a different level")
        void ignoresDifferentLevel() {
            RuleDefinition rule = rateRule("component-error-rate", RuleType.COMPONENT_LOG_RATE, "ERROR", 60, 1, 60);

            consumer.checkComponentLogRate(rule, entry("orders", "INFO", base, "i1"));
            consumer.checkComponentLogRate(rule, entry("orders", "INFO", base.plusSeconds(10), "i2"));

            verify(alertRepository, never()).save(any());
        }

        @Test
        @DisplayName("respects cooldown and does not spam alerts")
        void respectsCooldown() {
            RuleDefinition rule = rateRule("component-error-rate", RuleType.COMPONENT_LOG_RATE, "ERROR", 60, 1, 60);

            consumer.checkComponentLogRate(rule, entry("orders", "ERROR", base, "e1"));
            consumer.checkComponentLogRate(rule, entry("orders", "ERROR", base.plusSeconds(5), "e2"));
            consumer.checkComponentLogRate(rule, entry("orders", "ERROR", base.plusSeconds(10), "e3"));

            verify(alertRepository, times(1)).save(any());
        }

        @Test
        @DisplayName("two rate rules with same level but different windows don't interfere")
        void independentRulesSameLevel() {
            RuleDefinition tight = rateRule("rate-tight", RuleType.COMPONENT_LOG_RATE, "ERROR", 30, 1, 5);
            RuleDefinition wide = rateRule("rate-wide", RuleType.COMPONENT_LOG_RATE, "ERROR", 300, 5, 5);

            consumer.checkComponentLogRate(tight, entry("orders", "ERROR", base, "e1"));
            consumer.checkComponentLogRate(wide, entry("orders", "ERROR", base, "e1"));

            consumer.checkComponentLogRate(tight, entry("orders", "ERROR", base.plusSeconds(10), "e2"));
            consumer.checkComponentLogRate(wide, entry("orders", "ERROR", base.plusSeconds(10), "e2"));

            verify(alertRepository, times(1)).save(any());
        }
    }


    @Nested
    @DisplayName("checkOverallLogRate")
    class CheckOverallLogRateTests {

        @Test
        @DisplayName("counts all levels regardless of type")
        void countsAllLevels() {
            RuleDefinition rule = rateRule("overall-log-rate", RuleType.OVERALL_LOG_RATE, null, 60, 2, 60);

            consumer.checkOverallLogRate(rule, entry("orders", "INFO", base, "m1"));
            consumer.checkOverallLogRate(rule, entry("orders", "WARNING", base.plusSeconds(10), "m2"));
            consumer.checkOverallLogRate(rule, entry("orders", "ERROR", base.plusSeconds(20), "m3"));

            verify(alertRepository, times(1)).save(any());
        }

        @Test
        @DisplayName("tracks components independently")
        void tracksComponentsIndependently() {
            RuleDefinition rule = rateRule("overall-log-rate", RuleType.OVERALL_LOG_RATE, null, 60, 1, 60);

            consumer.checkOverallLogRate(rule, entry("orders", "INFO", base, "a1"));
            consumer.checkOverallLogRate(rule, entry("users", "INFO", base.plusSeconds(5), "b1"));

            verify(alertRepository, never()).save(any());
        }

        @Test
        @DisplayName("two overall rules with different windows don't interfere")
        void independentRulesDifferentWindows() {
            RuleDefinition rule300 = rateRule("overall-300", RuleType.OVERALL_LOG_RATE, null, 300, 5, 10);
            RuleDefinition rule60 = rateRule("overall-60", RuleType.OVERALL_LOG_RATE, null, 60, 1, 10);

            consumer.checkOverallLogRate(rule300, entry("orders", "INFO", base, "m1"));
            consumer.checkOverallLogRate(rule60, entry("orders", "INFO", base, "m1"));

            consumer.checkOverallLogRate(rule300, entry("orders", "INFO", base.plusSeconds(10), "m2"));
            consumer.checkOverallLogRate(rule60, entry("orders", "INFO", base.plusSeconds(10), "m2"));

            verify(alertRepository, times(1)).save(any());
        }
    }


    private RuleDefinition rateRule(String name, RuleType type, String level,
                                    int windowSeconds, int threshold, int coolDown) {
        RuleDefinition rule = new RuleDefinition();
        rule.setName(name);
        rule.setType(type);
        rule.setLevel(level);
        rule.setWindowSeconds(windowSeconds);
        rule.setThreshold(threshold);
        rule.setCoolDown(coolDown);
        return rule;
    }
}