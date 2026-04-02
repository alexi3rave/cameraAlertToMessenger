CURRENT STATE

обновлено: 2026-04-03

---

## Статус этапов

Stage 1 — ЗАКРЫТ 
Stage 2 — ЗАКРЫТ 
Stage 3 — ЗАКРЫТ 

текущая цель: Stage 4 (UI)

---

## Развернутые сервисы

camera-v2-postgres
camera-v2-watcher
camera-v2-processor
camera-v2-processor2
camera-v2-retry
camera-v2-ftp-cleanup
camera-ftp
camera-n8n

repo на сервере: /opt/cursor-agent/camera-system-v2
ветка: master

---

## Что реализовано

### pipeline v2 (Stage 1–3)

events registry
deliveries registry
routing: camera_routes → site.default_recipient → tenant.default_recipient
retry_worker: обработка failed_retryable с backoff + reaper stale processing по locked_at
ftp_cleanup_worker: TTL = 7 дней + отдельный quarantine TTL (FTP_QUARANTINE_RETENTION_DAYS)

auto onboarding: новая камера и site создаются автоматически
SHADOW_MODE: dry_run / shadow / prod
статус contracts зафиксированы (см. DB model.md)
базовая конфигурация: env/app.env + overrides через env в docker-compose.yml (для watcher/processor)

restart контейнеров не ломает pipeline
n8n контур не затронут

### watcher (оптимизирован 2026-03-29)

mtime-курсор не используется как фильтр silent-skip (чтобы не терять события после даунтайма)
seen_keys: O(1) дедупликация по fingerprint (camera_code + file_name + size + checksum_sha256)
seen_names: O(1) дедупликация по (camera_code, file_name) — исключает дубли при обновлении mtime FTP-сервером
слишком старые файлы не теряются: создаются в events как quarantine (quarantine_reason=too_old)
persistent DB connection (reconnect on failure)
быстрые дефолты: stabilize=0.2s, loop=1s
latency: ~3 минуты → <1 секунды (FTP → MAX)

дополнительно (2026-04-03):
минимальный размер файла перед вставкой в events: WATCH_MIN_FILE_SIZE_BYTES (default: 1024)
в журнал DISCOVERED добавлено поле file_size

### processor (обновлен 2026-04-03)

test file support: TEST_FILE_REGEX (default: (^test_|_test_|healthcheck|probe))
text-only уведомление для тестовых файлов: «✅ Test event received»
фото загружается только для реальных событий

дополнительно (2026-04-03):
preflight перед загрузкой фото в MAX (для защиты от обрезанных/недописанных файлов):
PROCESSOR_PREFLIGHT_ENABLED (default: 1)
PROCESSOR_PREFLIGHT_MIN_SIZE_BYTES (default: 1024)
PROCESSOR_PREFLIGHT_STABILIZE_SECONDS (default: 0.2)
JPEG: проверка SOI/EOI; PNG: проверка сигнатуры
опционально: PROCESSOR_PREFLIGHT_DECODE_VERIFY=1 (Pillow Image.verify/load)
при провале preflight событие уходит в failed_retryable (через retry policy), фото не отправляется

debug MAX API (через env в compose):
MAX_DEBUG_LOG=1
MAX_DEBUG_LOG_BODY_LIMIT (default: 512)

latency follow-up (LATENCY_DEBUG_FOLLOWUP=1):
в MAX отправляется короткое сообщение только с message_sent_at и end_to_end_ms

### журнал событий (добавлен 2026-03-30)

единый текстовый файл: logs/events/events.log
записывают все три сервиса: watcher, processor, ftp_cleanup
ротация: 10 МБ × 10 файлов

записи в журнале:
DISCOVERED — watcher: новый файл обнаружен и принят в обработку
SENT        — processor: уведомление доставлено в MAX
QUARANTINE  — processor: событие ушло в карантин (нет маршрута, нет chat_id)
FAILED      — processor: ошибка доставки (нет API-ключа, неверный режим и т.п.)
ERROR       — watcher / processor / ftp_cleanup: необработанное исключение
DELETED     — ftp_cleanup: файл физически удалён с диска

настройка через env:
EVENT_JOURNAL_DIR (default: /events → хост: logs/events/)
EVENT_JOURNAL_MAX_BYTES (default: 10 МБ)
EVENT_JOURNAL_BACKUP_COUNT (default: 10)

отдельный операционный лог очистки: logs/ftp_cleanup/ftp_cleanup.log
(содержит BATCH-итоги, REMOVE_ERROR, ALREADY_GONE)

### репо

Dockerfile + requirements.txt добавлены
.gitignore добавлен
RUNBOOK.md добавлен (операционные команды)
event_journal.py добавлен (shared journal module)

---

## Контракты (зафиксированы)

events.status:
discovered → ready → processing → sent / failed_retryable → quarantine

deliveries.status:
pending → started → sent / failed

pending — единственный начальный статус
uploaded — не используется

---

## Что осталось

Stage 4:
UI управления — маршруты, получатели, камеры, просмотр событий, ручные действия по quarantine

После Stage 3 (в приоритете):
отключение n8n — оба контура сейчас посылают уведомления параллельно; нужен migration plan
simplified schema Phase B/C — переключить runtime с legacy на photo_sources/photo_events
мониторинг — метрики (lag, error rate), алерты на quarantine и failed_retryable

Низкий приоритет:
multi-FTP — несколько источников с отдельными маршрутами
health-check токена MAX
регламент ротации токенов

---

## Известные ограничения

нет UI — правки маршрутов и получателей через SQL
n8n параллельно — риск дублирующих уведомлений при совпадении получателей
доступ к env/app.env ограничен правами на сервере (chmod 600)