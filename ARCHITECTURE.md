ARCHITECTURE

pipeline v2:

camera
↓
FTP
↓
watcher
↓
events (postgres)
↓
processor
↓
deliveries
↓
MAX API

старый pipeline продолжает работать:

camera
↓
FTP
↓
n8n
↓
MAX

---

компоненты:

watcher

обнаруживает файлы (сканирует WATCH_PATH, без silent-drop)
проверяет стабильность файла (stabilize timeout)
извлекает camera_code
определяет site
создает event (обычно ready, для слишком старых файлов quarantine с reason=too_old)
дедупликация: seen_keys (camera_code+file_name+size+checksum) + seen_names (camera_code+filename)
persistent DB connection

---

processor

определяет recipients (camera_routes → site default → tenant default)
создает deliveries
вызывает MAX API (текст + изображение)
тестовые файлы (TEST_FILE_REGEX): text-only уведомление без загрузки фото

---

retry_worker

обрабатывает events со статусом failed_retryable
повторяет отправку
использует backoff
reaper: возвращает stale processing в failed_retryable по locked_at

---

ftp_cleanup_worker

удаляет файлы старше TTL
TTL = 7 дней
для quarantine поддерживается отдельный TTL (FTP_QUARANTINE_RETENTION_DAYS)

после удаления:
events.ftp_removed_at заполняется

---

postgres

source of truth

---

docker compose

camera-v2-postgres
camera-v2-watcher
camera-v2-processor
camera-v2-retry
camera-v2-ftp-cleanup
camera-ftp
camera-n8n

---

event_journal

единый текстовый журнал: logs/events/events.log
пишут: watcher, processor, ftp_cleanup
формат: TIMESTAMP  SERVICE  ACTION  field=value ...
действия: DISCOVERED / SENT / QUARANTINE / FAILED / ERROR / DELETED
ротация: 10 МБ × 10 файлов (RotatingFileHandler)
модуль: app/event_journal.py (shared, импортируется всеми сервисами)

---

ключевой принцип:

состояние хранится в БД
сервисы stateless