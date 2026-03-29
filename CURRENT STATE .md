CURRENT STATE

обновлено: 2026-03-30

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
camera-v2-retry
camera-v2-ftp-cleanup
camera-ftp
camera-n8n

repo на сервере: /opt/cursor-agent/camera-system-v2
ветка: feat/watcher-perf-dedup-test-events

---

## Что реализовано

### pipeline v2 (Stage 1–3)

events registry
deliveries registry
routing: camera_routes → site.default_recipient → tenant.default_recipient
retry_worker: обработка failed_retryable с backoff
ftp_cleanup_worker: TTL = 7 дней
auto onboarding: новая камера и site создаются автоматически
SHADOW_MODE: dry_run / shadow / prod
статус contracts зафиксированы (см. DB model.md)
single env файл: env/app.env
restart контейнеров не ломает pipeline
n8n контур не затронут

### watcher (оптимизирован 2026-03-29)

mtime-курсор: сканируется только новые файлы (WATCH_USE_MTIME_CURSOR=1)
seen_keys: O(1) дедупликация по fingerprint (camera_code + file_name + size + mtime)
seen_names: O(1) дедупликация по (camera_code, file_name) — исключает дубли при обновлении mtime FTP-сервером
persistent DB connection (reconnect on failure)
быстрые дефолты: stabilize=0.2s, loop=1s
latency: ~3 минуты → <1 секунды (FTP → MAX)

### processor (обновлен 2026-03-29)

test file support: TEST_FILE_REGEX (default: \.txt$)
text-only уведомление для тестовых файлов: «✅ Test event received»
фото загружается только для реальных событий

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