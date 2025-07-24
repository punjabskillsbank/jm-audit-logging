package com.jobmatrix.JmAuditLogging.repository;

import com.jobmatrix.JmAuditLogging.entity.AuditLog;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AuditLogRepository extends JpaRepository<AuditLog, Long> {
}
