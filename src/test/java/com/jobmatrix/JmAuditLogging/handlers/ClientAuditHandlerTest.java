package com.jobmatrix.JmAuditLogging.handlers;

import com.jobmatrix.JmAuditLogging.dto.AuditLogDTO;
import com.jobmatrix.JmAuditLogging.entity.AuditLog;
import com.jobmatrix.JmAuditLogging.enums.EventType;
import com.jobmatrix.JmAuditLogging.repository.AuditLogRepository;
import com.jobmatrix.JmAuditLogging.test_utils.factory.AuditLogsTestDataFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.modelmapper.ModelMapper;
import org.modelmapper.TypeMap;
import org.springframework.kafka.support.Acknowledgment;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ClientAuditHandlerTest {

    @Mock
    private AuditLogRepository auditLogRepository;

    @Mock
    private ModelMapper modelMapper;

    @Mock
    private Acknowledgment ack;

    @Mock
    private TypeMap<AuditLogDTO, AuditLog> typeMap;

    @InjectMocks
    private ClientAuditHandler clientAuditHandler;

    private AuditLogDTO testDto;
    private AuditLog testAuditLog;

    @BeforeEach
    void setUp() {
        testDto = AuditLogsTestDataFactory.sampleClientAuditLogDTO();
        testAuditLog = AuditLogsTestDataFactory.sampleClientAuditLogEntity();
    }

    private void setupModelMapperMocks() {
        when(modelMapper.typeMap(AuditLogDTO.class, AuditLog.class)).thenReturn(typeMap);
        when(modelMapper.map(testDto, AuditLog.class)).thenReturn(testAuditLog);
    }

    @Test
    void handleAudit_validDto_savesAndAcknowledges() throws Exception {
        // Arrange
        setupModelMapperMocks();
        String key = "client-key";
        int partition = 1;
        long offset = 456L;

        // Act
        clientAuditHandler.handleAudit(testDto, ack, key, partition, offset);

        // Assert
        verify(modelMapper).typeMap(AuditLogDTO.class, AuditLog.class);
        verify(modelMapper).map(testDto, AuditLog.class);

        verify(auditLogRepository).save(testAuditLog);
        verify(ack).acknowledge();

        assertEquals(EventType.CLIENT_PROFILE_UPDATED, testAuditLog.getEventType());
        assertEquals("Client", testAuditLog.getEntityName());
    }

    @Test
    void handleAudit_saveFails_throwsExceptionAndDoesNotAcknowledge() {
        // Arrange
        setupModelMapperMocks();
        String key = "client-key";
        int partition = 1;
        long offset = 456L;

        RuntimeException expectedException = new RuntimeException("Database error");
        when(auditLogRepository.save(any(AuditLog.class))).thenThrow(expectedException);

        // Act & Assert
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            clientAuditHandler.handleAudit(testDto, ack, key, partition, offset);
        });

        assertEquals(expectedException, exception);
        verify(auditLogRepository).save(any(AuditLog.class));
        verify(ack, never()).acknowledge();
    }

    @Test
    void handleAudit_nullDto_throwsException() {
        // Act & Assert
        assertThrows(NullPointerException.class, () -> {
            clientAuditHandler.handleAudit(null, ack, "key", 0, 1L);
        });
    }

    @Test
    void handleAudit_nullAck_throwsException() {
        // Arrange
        setupModelMapperMocks();

        // Act & Assert
        assertThrows(NullPointerException.class, () -> {
            clientAuditHandler.handleAudit(testDto, null, "key", 0, 1L);
        });
    }
}
