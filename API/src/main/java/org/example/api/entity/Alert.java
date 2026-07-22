package org.example.api.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "alerts")
public class Alert {

    @Id
    private Long id;

    private String ruleName;
    private String componentName;

    @Column(length = 2000)
    private String description;

    private LocalDateTime createdAt;

    public Alert() {}

    public Long getId() { return id; }
    public String getRuleName() { return ruleName; }
    public String getComponentName() { return componentName; }
    public String getDescription() { return description; }
    public LocalDateTime getCreatedAt() { return createdAt; }
}