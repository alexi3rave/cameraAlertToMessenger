# Camera Alert System v2.1.0 Release Notes

**Release Date:** April 1, 2026  
**Version:** 2.1.0 (Minor release - new features and reliability improvements)

## Overview

This release focuses on **reliability hardening** and **multi-tenant security** improvements for the camera event notification pipeline. All changes maintain backward compatibility while significantly improving system robustness and data integrity.

---

## 🔧 New Features

### Database Guardrails (Migration 007)
- **FTP Root Overlap Prevention**: Automatic detection and blocking of overlapping FTP source paths
- **Tenant Isolation Enforcement**: Database triggers prevent cross-tenant routing violations
- **Routing Integrity**: Validates tenant alignment for camera routes and default recipients

### Enhanced MAX API Client
- **Bounded Retry Logic**: Configurable retry attempts with exponential backoff
- **HTTP Status Awareness**: Proper handling of 429 (rate limit) and 5xx server errors
- **Retry-After Header Support**: Respects server-provided retry delays

---

## 🛡️ Reliability Improvements

### Watcher Service
- **No Silent Drops**: Old files are quarantined instead of being silently ignored
- **Improved Deduplication**: Uses checksum-based fingerprinting (SHA256) instead of mtime
- **Quarantine Handling**: Files older than threshold get `quarantine` status with `reason=too_old`

### Processor Service  
- **Worker Lock Metadata**: Processing events include lock owner and timestamp
- **Tenant-Aligned Routing**: Enforced tenant scoping in event fetching queries
- **Structured Logging**: Enhanced log fields (event_id, recipient_id, attempt_no, trace_id)

### Retry Worker
- **Reaper Logic**: Automatically recovers stuck `processing` events based on lock timeout
- **Dual-Write Sync**: Mirrors status updates to both `events` and `photo_events` tables
- **Batch Logging**: Structured event journal entries for requeue/quarantine/reaper operations

### FTP Cleanup Worker
- **Separate Quarantine Retention**: Configurable retention period for quarantined files
- **Migration Safety**: Runtime check for required database columns before startup

---

## 📊 Configuration Updates

### New Environment Variables
```bash
# MAX API Retry Configuration
MAX_RETRY_ATTEMPTS=3
MAX_RETRY_BASE_DELAY_SECONDS=1.0
MAX_RETRY_MAX_DELAY_SECONDS=60.0

# FTP Cleanup Retention
FTP_QUARANTINE_RETENTION_DAYS=14  # Default: same as RETENTION_DAYS
```

---

## 🗄️ Database Changes

### Migration 007: Tenant Guardrails and FTP Overlap Prevention
- **Functions Added:**
  - `_norm_path()` - Path normalization utility
  - `trg_ftp_sources_no_overlap()` - Prevents overlapping FTP roots
  - `trg_camera_routes_tenant_guard()` - Enforces tenant alignment in routes
  - `trg_sites_default_recipient_tenant_guard()` - Validates site default recipients
  - `trg_tenants_default_recipient_tenant_guard()` - Validates tenant default recipients

- **Triggers Added:**
  - `ftp_sources_no_overlap` on `ftp_sources` table
  - `camera_routes_tenant_guard` on `camera_routes` table  
  - `sites_default_recipient_tenant_guard` on `sites` table
  - `tenants_default_recipient_tenant_guard` on `tenants` table

---

## 📚 Documentation Updates

### Updated Documents
- **STATUS_CONTRACT.md**: Aligned with runtime behavior (ready/quarantine statuses)
- **DB model.md**: Updated dedup key specification and added lock fields
- **ARCHITECTURE.md**: Reflects no-silent-drop policy and checksum deduplication
- **CURRENT STATE.md**: Updated service descriptions and retention policies

---

## 🔄 Breaking Changes

**None** - This release maintains full backward compatibility.

---

## 🐛 Bug Fixes

- **Fixed**: Silent dropping of old files during watcher downtime
- **Fixed**: Inconsistent deduplication leading to duplicate events
- **Fixed**: Missing tenant enforcement in processor routing queries
- **Fixed**: Stuck processing events not being recovered
- **Fixed**: FTP cleanup worker crashes due to missing database columns

---

## 🚀 Deployment Notes

### Prerequisites
- Apply migration `007_tenant_guardrails_and_ftp_overlap.sql`
- Restart `camera-v2-processor` container to load new MAX client

### Rollback Plan
- Migration 007 can be rolled back by dropping the created triggers and functions
- Code changes are backward compatible and can be reverted without data loss

---

## 📈 Performance Impact

- **Positive**: Reduced duplicate processing due to improved deduplication
- **Positive**: Faster recovery from stuck states via reaper logic
- **Minimal**: Database trigger overhead is negligible for normal operations
- **Configurable**: MAX API retry delays are tunable per environment

---

## 🔍 Monitoring Recommendations

Monitor the following metrics post-deployment:
- Quarantine event count (should capture previously silent-dropped files)
- Reaper recovery operations (stuck processing → failed_retryable transitions)
- MAX API retry attempts and success rates
- Database constraint violations (should be zero with proper tenant setup)

---

## 👥 Credits

**Implementation Team**: Reliability Engineering  
**Review**: Architecture Review Board  
**Testing**: End-to-end smoke tests completed  
**Deployment**: Production deployment verified

---

## 📞 Support

For issues related to this release:
- Check service logs in `/logs/events/events.log`
- Verify database constraint violations in PostgreSQL logs
- Review MAX API retry patterns in processor logs

**Next Release**: v2.2.0 (planned UI management features)
