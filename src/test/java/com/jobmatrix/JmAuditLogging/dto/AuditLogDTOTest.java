package com.jobmatrix.JmAuditLogging.dto;

import com.jobmatrix.JmAuditLogging.test_utils.factory.AuditLogsTestDataFactory;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class AuditLogDTOTest {

    @Test
    void testAllArgsConstructorAndGetters() {
        UUID userId = UUID.randomUUID();
        AuditLogDTO dto = new AuditLogDTO(
                "job-posting",
                userId,
                101L,
                "{\"title\":\"Old Title\"}",
                "{\"title\":\"New Title\"}"
        );

        assertEquals("job-posting", dto.getServiceName());
        assertEquals(userId, dto.getUserId());
        assertEquals(101L, dto.getEntityId());
        assertEquals("{\"title\":\"Old Title\"}", dto.getOldData());
        assertEquals("{\"title\":\"New Title\"}", dto.getNewData());
    }

    @Test
    void testBuilderPattern() {
        UUID userId = UUID.randomUUID();
        AuditLogDTO dto = AuditLogDTO.builder()
                .serviceName("job-posting")
                .userId(userId)
                .entityId(202L)
                .oldData("{\"a\":1}")
                .newData("{\"a\":2}")
                .build();

        assertNotNull(dto);
        assertEquals("job-posting", dto.getServiceName());
        assertEquals(userId, dto.getUserId());
        assertEquals(202L, dto.getEntityId());
        assertEquals("{\"a\":1}", dto.getOldData());
        assertEquals("{\"a\":2}", dto.getNewData());
    }

    @Test
    void testNoArgsConstructorAndSetters() {
        AuditLogDTO dto = new AuditLogDTO();
        UUID userId = UUID.randomUUID();

        dto.setServiceName("proposal");
        dto.setUserId(userId);
        dto.setEntityId(303L);
        dto.setOldData("{\"x\":1}");
        dto.setNewData("{\"x\":2}");

        assertEquals("proposal", dto.getServiceName());
        assertEquals(userId, dto.getUserId());
        assertEquals(303L, dto.getEntityId());
        assertEquals("{\"x\":1}", dto.getOldData());
        assertEquals("{\"x\":2}", dto.getNewData());
    }

    @Test
    void testToBuilderCreatesCopy() {
        AuditLogDTO original = AuditLogsTestDataFactory.sampleJobPostingAuditLogDTO();

        AuditLogDTO copy = original.toBuilder().build();

        assertEquals(original.getServiceName(), copy.getServiceName());
        assertEquals(original.getEntityId(), copy.getEntityId());
        assertEquals(original.getUserId(), copy.getUserId());
        assertEquals(original.getOldData(), copy.getOldData());
        assertEquals(original.getNewData(), copy.getNewData());
        assertNotSame(original, copy); // Ensure different instances
    }
}
