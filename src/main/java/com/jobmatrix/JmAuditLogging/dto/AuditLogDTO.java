package com.jobmatrix.JmAuditLogging.dto;

import com.jobmatrix.JmAuditLogging.enums.EventType;
import lombok.*;

import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class AuditLogDTO {

    private String serviceName;
    private UUID userId;
    private Long entityId;
    private String oldData;
    private String newData;

}
