DATABASE MODEL

основная модель:

file → event → deliveries

---

EVENT STATUS CONTRACT (зафиксировано)

discovered
ready
processing
sent
failed_retryable
quarantine

runtime note:
- watcher пишет новые события сразу в ready
- watcher пишет слишком старые файлы в quarantine (quarantine_reason=too_old)

---

DELIVERY STATUS CONTRACT (зафиксировано)

pending
started
sent
failed

pending используется как единый начальный статус
uploaded НЕ используется

---

events

id
camera_id
site_id
file_path
file_name
file_size
checksum
status
attempt_count
locked_at
lock_owner
next_retry_at
quarantine_reason
created_at
processed_at
ftp_removed_at

---

deliveries

id
event_id
recipient_id
status
attempt_count
provider_message_id
error_text
next_retry_at
created_at
sent_at

---

cameras

id
camera_code
site_id
status

---

sites

id
code
status

---

recipients

id
chat_id
messenger_type
status

---

dedup key:

camera_code
file_name
file_size
checksum_sha256

---

routing:

camera → site → recipients