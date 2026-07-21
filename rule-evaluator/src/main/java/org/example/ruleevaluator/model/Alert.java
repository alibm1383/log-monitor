package org.example.ruleevaluator.model;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "alerts")
public class Alert {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String ruleName;
    private String componentName;

    @Column(length = 2000)
    private String description;

    private LocalDateTime createdAt;

    public Alert() {}

    public Alert(String ruleName, String componentName, String description, LocalDateTime createdAt) {
        this.ruleName = ruleName;
        this.componentName = componentName;
        this.description = description;
        this.createdAt = createdAt;
    }

    public Long getId() { return id; }

    public String getRuleName() { return ruleName; }
    public void setRuleName(String ruleName) { this.ruleName = ruleName; }

    public String getComponentName() { return componentName; }
    public void setComponentName(String componentName) { this.componentName = componentName; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
}