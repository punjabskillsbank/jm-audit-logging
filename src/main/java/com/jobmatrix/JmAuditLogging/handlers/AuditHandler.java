package com.jobmatrix.JmAuditLogging.handlers;

import com.jobmatrix.JmAuditLogging.dto.AuditLogDTO;
import org.springframework.kafka.support.Acknowledgment;

public interface AuditHandler {
    void handleAudit(AuditLogDTO dto, Acknowledgment ack, String key, int partition, long offset) throws Exception;
}
