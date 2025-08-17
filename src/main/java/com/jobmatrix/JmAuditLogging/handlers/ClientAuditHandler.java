package com.jobmatrix.JmAuditLogging.handlers;

import com.jobmatrix.JmAuditLogging.dto.AuditLogDTO;
import com.jobmatrix.JmAuditLogging.entity.AuditLog;
import com.jobmatrix.JmAuditLogging.enums.EventType;
import com.jobmatrix.JmAuditLogging.repository.AuditLogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component("client")
@RequiredArgsConstructor
public class ClientAuditHandler implements AuditHandler {

    private final AuditLogRepository auditLogRepository;
    private final ModelMapper modelMapper;

    @Override
    public void handleAudit(AuditLogDTO dto, Acknowledgment ack, String key, int partition, long offset) throws Exception {
        // Configure ModelMapper for old/new data mapping
        modelMapper.typeMap(AuditLogDTO.class, AuditLog.class).addMappings(mapper -> {
            mapper.map(AuditLogDTO::getOldData, AuditLog::setOldValue);
            mapper.map(AuditLogDTO::getNewData, AuditLog::setNewValue);
        });

        // Map DTO to entity
        AuditLog auditLog = modelMapper.map(dto, AuditLog.class);

        // Set fixed fields
        auditLog.setEventType(EventType.CLIENT_PROFILE_UPDATED);
        auditLog.setEntityName("Client");

        try {
            auditLogRepository.save(auditLog);
            log.info("Audit log saved for client ID: {}", dto.getEntityId());

            ack.acknowledge(); // Kafka message acknowledged
            log.info("Acknowledged message - Key: {}, Partition: {}, Offset: {}",
                    key, partition, offset);
        } catch (Exception e) {
            log.error("Error processing audit log - Key: {}, Partition: {}, Offset: {}: {}",
                    key, partition, offset, e.getMessage(), e);
            throw e; // Retry will be triggered if configured
        }
    }
}
