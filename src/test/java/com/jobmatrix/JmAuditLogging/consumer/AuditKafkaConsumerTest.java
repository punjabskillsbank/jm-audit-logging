package com.jobmatrix.JmAuditLogging.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobmatrix.JmAuditLogging.dto.AuditLogDTO;
import com.jobmatrix.JmAuditLogging.entity.AuditLog;
import com.jobmatrix.JmAuditLogging.repository.AuditLogRepository;
import com.jobmatrix.JmAuditLogging.test_utils.factory.AuditLogsTestDataFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.modelmapper.ModelMapper;
import org.modelmapper.TypeMap;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AuditKafkaConsumerTest {

    @InjectMocks
    private AuditKafkaConsumer consumer;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private AuditLogRepository auditLogRepository;

    @Mock
    private ModelMapper modelMapper;

    @Mock
    private Acknowledgment ack;

    private AuditLogDTO dto;
    private String messageJson;
    private AuditLog entity;

    @BeforeEach
    void setup() throws Exception {
        dto = AuditLogsTestDataFactory.sampleJobPostingAuditLogDTO();
        entity = AuditLogsTestDataFactory.sampleAuditLogEntity();
        messageJson = AuditLogsTestDataFactory.sampleJobPostingKafkaJson();
    }

    @Test
    void testConsume_validJobPostingAudit_savesToDatabaseAndAcknowledges() throws Exception {
        // Arrange
        JsonNode rootNode = new ObjectMapper().readTree(messageJson);
        when(objectMapper.readTree(messageJson)).thenReturn(rootNode);
        
        TypeMap<AuditLogDTO, AuditLog> typeMap = mock(TypeMap.class);
        when(modelMapper.typeMap(AuditLogDTO.class, AuditLog.class)).thenReturn(typeMap);
        when(modelMapper.map(any(AuditLogDTO.class), eq(AuditLog.class))).thenReturn(entity);

        // Act
        consumer.consume(messageJson, "key1", 0, 123L, ack);

        // Assert
        verify(auditLogRepository, times(1)).save(any(AuditLog.class));
        verify(ack, times(1)).acknowledge();
    }

    @Test
    void testConsume_unrecognizedServiceName_acknowledgesButDoesNotSave() throws Exception {
        // Arrange
        String unknownServiceJson = AuditLogsTestDataFactory.sampleKafkaJsonWithService("unknown-service");
        
        JsonNode rootNode = new ObjectMapper().readTree(unknownServiceJson);
        when(objectMapper.readTree(unknownServiceJson)).thenReturn(rootNode);

        // Act
        consumer.consume(unknownServiceJson, "key2", 1, 456L, ack);

        // Assert
        verify(auditLogRepository, never()).save(any());
        verify(ack, times(1)).acknowledge();
    }

    @Test
    void testConsume_exceptionWhileProcessing_throwsExceptionAndDoesNotAcknowledge() throws Exception {
        // Arrange
        JsonNode rootNode = new ObjectMapper().readTree(messageJson);
        when(objectMapper.readTree(messageJson)).thenReturn(rootNode);
        when(modelMapper.typeMap(any(), any())).thenThrow(new RuntimeException("Mapping failed"));

        // Act + Assert
        try {
            consumer.consume(messageJson, "key3", 0, 789L, ack);
        } catch (Exception e) {
            // expected
        }

        verify(ack, never()).acknowledge();
        verify(auditLogRepository, never()).save(any());
    }
}
