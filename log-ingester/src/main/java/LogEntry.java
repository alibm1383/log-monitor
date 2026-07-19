import java.time.LocalDateTime;

public class LogEntry {
    private String component;
    private LocalDateTime timestamp;
    private String thread;
    private String level;
    private String logger;
    private String message;

    public LogEntry() {}

    public LogEntry(String component, LocalDateTime timestamp, String thread,
                    String level, String logger, String message) {
        this.component = component;
        this.timestamp = timestamp;
        this.thread = thread;
        this.level = level;
        this.logger = logger;
        this.message = message;
    }

    // getter و setter برای همه فیلدها (Jackson برای serialize بهشون نیاز داره)
    public String getComponent() { return component; }
    public void setComponent(String component) { this.component = component; }

    public LocalDateTime getTimestamp() { return timestamp; }
    public void setTimestamp(LocalDateTime timestamp) { this.timestamp = timestamp; }

    public String getThread() { return thread; }
    public void setThread(String thread) { this.thread = thread; }

    public String getLevel() { return level; }
    public void setLevel(String level) { this.level = level; }

    public String getLogger() { return logger; }
    public void setLogger(String logger) { this.logger = logger; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
}