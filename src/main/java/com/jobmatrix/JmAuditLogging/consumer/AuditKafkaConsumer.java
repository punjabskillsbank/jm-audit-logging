package com.jobmatrix.JmAuditLogging.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobmatrix.JmAuditLogging.dto.AuditLogDTO;
import com.jobmatrix.JmAuditLogging.handlers.AuditHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class AuditKafkaConsumer {

    private final ObjectMapper objectMapper;
    private final Map<String, AuditHandler> auditHandlers;

    @KafkaListener(topics = "${audit.logging.topic}", groupId = "${spring.kafka.consumer.group-id}", concurrency = "1")
    public void consume(@Payload String message, 
                       @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key,
                       @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                       @Header(KafkaHeaders.OFFSET) long offset,
                       Acknowledgment ack) throws Exception {
        log.info("Received Kafka message - Key: {}, Partition: {}, Offset: {}, Message: {}",
                key, partition, offset, message);

        try {
            // First, read the message as a JsonNode to handle the object fields
            com.fasterxml.jackson.databind.JsonNode rootNode = objectMapper.readTree(message);
            
            // Create a new object to build the DTO
            AuditLogDTO dto = new AuditLogDTO();
            
            // Set simple fields
            if (rootNode.has("serviceName")) {
                dto.setServiceName(rootNode.get("serviceName").asText());
            }
            if (rootNode.has("userId")) {
                dto.setUserId(UUID.fromString(rootNode.get("userId").asText()));
            }
            if (rootNode.has("entityId")) {
                dto.setEntityId(rootNode.get("entityId").asLong());
            }
            
            // Handle oldData and newData as JSON strings
            if (rootNode.has("oldData")) {
                dto.setOldData(rootNode.get("oldData").toString());
            }
            if (rootNode.has("newData")) {
                dto.setNewData(rootNode.get("newData").toString());
            }

            String serviceName = dto.getServiceName().toLowerCase();
            AuditHandler handler = auditHandlers.get(serviceName);
            
            if (handler != null) {
                handler.handleAudit(dto, ack, key, partition, offset);
            } else {
                log.warn("No handler found for service: {}", serviceName);
                ack.acknowledge();
            }
        } catch (Exception e) {
            log.error("Error processing message - Key: {}, Partition: {}, Offset: {}: {}",
                    key, partition, offset, e.getMessage(), e);
            throw e; // Will trigger retry if configured
        }
    }


}

