package com.jobmatrix.JmAuditLogging.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobmatrix.JmAuditLogging.dto.AuditLogDTO;
import com.jobmatrix.JmAuditLogging.handlers.AuditHandler;
import com.jobmatrix.JmAuditLogging.handlers.JobPostingAuditHandler;
import com.jobmatrix.JmAuditLogging.test_utils.factory.AuditLogsTestDataFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AuditKafkaConsumerTest {

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private JobPostingAuditHandler jobPostingAuditHandler;

    @Mock
    private AuditHandler clientAuditHandler;

    @Mock
    private Acknowledgment ack;
    
    private Map<String, AuditHandler> handlerMap;

    @InjectMocks
    private AuditKafkaConsumer consumer;

    private String messageJson;
    private AuditLogDTO testDto;

    @Captor
    private ArgumentCaptor<AuditLogDTO> dtoCaptor;

    @BeforeEach
    void setup() throws Exception {
        messageJson = AuditLogsTestDataFactory.sampleJobPostingKafkaJson();
        testDto = AuditLogsTestDataFactory.sampleJobPostingAuditLogDTO();
        
        // Setup handler map with job-posting handler
        handlerMap = new HashMap<>();
        handlerMap.put("job-posting", jobPostingAuditHandler);
        handlerMap.put("client", clientAuditHandler);
        // Inject the handler map into the consumer
        consumer = new AuditKafkaConsumer(objectMapper, handlerMap);
    }

    @Test
    void consume_validJobPostingMessage_callsHandlerWithCorrectParameters() throws Exception {
        // Arrange
        String key = "test-key";
        int partition = 0;
        long offset = 123L;
        
        when(objectMapper.readTree(messageJson)).thenReturn(
            new ObjectMapper().readTree(messageJson)
        );

        // Act
        consumer.consume(messageJson, key, partition, offset, ack);

        // Assert
        verify(jobPostingAuditHandler, times(1))
            .handleAudit(dtoCaptor.capture(), eq(ack), eq(key), eq(partition), eq(offset));
        
        AuditLogDTO capturedDto = dtoCaptor.getValue();
        assertNotNull(capturedDto);
        assertEquals(testDto.getServiceName(), capturedDto.getServiceName());
        assertEquals(testDto.getEntityId(), capturedDto.getEntityId());
    }

    @Test
    void consume_clientEvent_entityIdIsNull() throws Exception {
        String clientJson = AuditLogsTestDataFactory.sampleClientKafkaJson();
        when(objectMapper.readTree(clientJson)).thenReturn(
                new ObjectMapper().readTree(clientJson)
        );

        consumer.consume(clientJson, "client-key", 0, 111L, ack);

        verify(clientAuditHandler, times(1))
                .handleAudit(dtoCaptor.capture(), eq(ack), eq("client-key"), eq(0), eq(111L));

        AuditLogDTO captured = dtoCaptor.getValue();
        assertNotNull(captured);
        assertEquals("client", captured.getServiceName());
        assertNull(captured.getEntityId()); // ✅ entityId should be null
    }


    @Test
    void consume_unrecognizedService_acknowledgesWithoutCallingHandler() throws Exception {
        // Arrange
        String unknownServiceJson = AuditLogsTestDataFactory.sampleKafkaJsonWithService("unknown-service");
        
        when(objectMapper.readTree(unknownServiceJson)).thenReturn(
            new ObjectMapper().readTree(unknownServiceJson)
        );

        // Act
        consumer.consume(unknownServiceJson, "key2", 1, 456L, ack);

        // Assert
        verify(jobPostingAuditHandler, never())
            .handleAudit(any(), any(), any(), anyInt(), anyLong());
        verify(ack, times(1)).acknowledge();
    }

    @Test
    void consume_jsonProcessingError_throwsException() throws Exception {
        // Arrange
        String invalidJson = "{invalid-json}";
        when(objectMapper.readTree(invalidJson)).thenThrow(new RuntimeException("Invalid JSON"));

        // Act & Assert
        assertThrows(RuntimeException.class, () -> {
            consumer.consume(invalidJson, "key3", 0, 789L, ack);
        });
        
        verify(ack, never()).acknowledge();
        verify(jobPostingAuditHandler, never())
            .handleAudit(any(), any(), any(), anyInt(), anyLong());
    }
    
    @Test
    void consume_handlerThrowsException_propagatesException() throws Exception {
        // Arrange
        when(objectMapper.readTree(messageJson)).thenReturn(
            new ObjectMapper().readTree(messageJson)
        );
        
        RuntimeException expectedException = new RuntimeException("Handler failed");
        doThrow(expectedException)
            .when(jobPostingAuditHandler)
            .handleAudit(any(), any(), any(), anyInt(), anyLong());

        // Act & Assert
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            consumer.consume(messageJson, "key4", 0, 789L, ack);
        });
        
        assertEquals(expectedException, exception);
        verify(ack, never()).acknowledge();
    }
}
