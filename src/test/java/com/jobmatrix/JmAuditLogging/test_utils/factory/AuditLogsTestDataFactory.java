package com.jobmatrix.JmAuditLogging.test_utils.factory;

import com.jobmatrix.JmAuditLogging.dto.AuditLogDTO;
import com.jobmatrix.JmAuditLogging.entity.AuditLog;
import com.jobmatrix.JmAuditLogging.enums.EventType;

import java.util.UUID;

public class AuditLogsTestDataFactory {

    private static final UUID SAMPLE_USER_ID = UUID.fromString("d290f1ee-6c54-4b01-90e6-d701748f0851");

    public static AuditLogDTO sampleJobPostingAuditLogDTO() {
        return AuditLogDTO.builder()
                .serviceName("job-posting")
                .entityId(101L)
                .userId(SAMPLE_USER_ID)
                .oldData("{\"title\": \"Old Title\"}")
                .newData("{\"title\": \"New Title\"}")
                .build();
    }

    public static AuditLog sampleAuditLogEntity() {
        AuditLog entity = new AuditLog();
        entity.setServiceName("job-posting");
        entity.setEntityId(101L);
        entity.setUserId(SAMPLE_USER_ID);
        entity.setOldValue("{\"title\": \"Old Title\"}");
        entity.setNewValue("{\"title\": \"New Title\"}");
        entity.setEventType(EventType.JOB_POSTING_UPDATED);
        entity.setEntityName("JobPosting");
        return entity;
    }

    public static String sampleJobPostingKafkaJson() {
        return """
                {
                  "serviceName": "job-posting",
                  "entityId": 101,
                  "userId": "d290f1ee-6c54-4b01-90e6-d701748f0851",
                  "oldData": { "title": "Old Title" },
                  "newData": { "title": "New Title" }
                }
                """;
    }

    public static String sampleKafkaJsonWithService(String serviceName) {
        return String.format("""
                {
                  "serviceName": "%s",
                  "entityId": 102,
                  "userId": "d290f1ee-6c54-4b01-90e6-d701748f0851",
                  "oldData": { "field": "old" },
                  "newData": { "field": "new" }
                }
                """, serviceName);
    }

    public static AuditLogDTO sampleInvalidAuditLogDTO() {
        // Intentionally missing some fields (e.g., serviceName and userId)
        return AuditLogDTO.builder()
                .entityId(999L)
                .oldData(null)
                .newData(null)
                .build();
    }
}
