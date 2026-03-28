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

обнаруживает файлы
проверяет стабильность файла
извлекает camera_code
определяет site
создает event

---

processor

определяет recipients
создает deliveries
вызывает MAX API (текст + изображение)

---

retry_worker

обрабатывает events со статусом failed_retryable
повторяет отправку
использует backoff

---

ftp_cleanup_worker

удаляет файлы старше TTL
TTL = 7 дней

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

ключевой принцип:

состояние хранится в БД
сервисы stateless