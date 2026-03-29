# Runbook: camera-system-v2

## Архитектура

- **Старый контур:** камера → FTP → n8n → MAX (`camera-n8n`).
- **Новый v2:** камера → FTP → `watcher` → `events` → `processor` → `deliveries` → MAX (`camera-v2-*`).

Оба контура читают один FTP-каталог на хосте. v2 **не отключает** n8n (параллельная работа).

---

## Текущий статус

### Что реализовано ✅

| Компонент | Описание |
|---|---|
| **watcher** | Сканирует FTP, вставляет события в `events`. mtime-курсор: обрабатывает только новые файлы (не полный rescan). In-memory дедупликация `seen_keys` + `seen_names` — исключает дубли даже при обновлении mtime FTP-сервером. Persistent DB connection. |
| **processor** | Забирает `ready` события, определяет получателя (camera_routes → site default → tenant default), отправляет фото или текст в MAX. Поддержка тестовых файлов (`TEST_FILE_REGEX`): text-only сообщение «✅ Test event received» без загрузки фото. |
| **retry_worker** | Повторные попытки по расписанию для `failed_retryable` событий. Политика retry — в `service_config` (JSON в БД). |
| **ftp_cleanup_worker** | Удаляет файлы с FTP после успешной отправки + TTL (`FTP_RETENTION_DAYS`). |
| **Маршрутизация** | Цепочка: camera_routes → site.default_recipient → tenant.default_recipient. |
| **SHADOW_MODE** | `dry_run` (без отправки), `shadow` (в тестовый чат), `prod` (реальная отправка). |
| **Статусы событий** | `discovered → ready → processing → sent / failed_retryable → quarantine` |
| **Статусы доставок** | `pending → started → sent / failed` |
| **Миграции** | `001`–`006`: init, delivery, patch, autorouting, ftp_cleanup, simplified schema, status alignment. |
| **Simplified schema Phase A** | Таблицы `photo_sources`, `photo_events` — dual-write из watcher (non-critical). Runtime на legacy-потоке. |
| **MAX API** | Загрузка фото + отправка сообщения. Auth: `Authorization: <token>` (без Bearer). Base: `https://platform-api.max.ru`. |
| **Docker build** | `Dockerfile` + `requirements.txt` в репо; все сервисы собираются из `app/`. |

### Что осталось 🔲

| Задача | Приоритет |
|---|---|
| **UI управления** — маршруты, получатели, камеры, просмотр событий | High |
| **Отключение n8n** — сейчас оба контура посылают уведомления по одним файлам; согласовать migration plan | Medium |
| **Simplified schema Phase B/C** — переключить runtime с legacy на `photo_sources/photo_events` | Medium |
| **Мониторинг** — метрики (lag, error rate), алерты на quarantine | Medium |
| **Multi-FTP** — несколько источников с отдельными маршрутами (сейчас через `WATCH_PATH`) | Low |

---

## Каталоги

- Compose: `compose/docker-compose.yml`
- Конфигурация: `env/app.env` (docker compose читает **этот** файл; `chmod 600`).
- Код сервисов: `app/`
- Миграции: `migrations/` (применяются при первом старте postgres; для существующей БД — вручную)
- FTP на хосте: `/home/alex/camera-system/ftp-uploads` → в контейнерах `/source`

---

## Основные команды

Из каталога `compose/` на сервере:

```bash
docker compose ps
docker compose logs watcher --tail 50
docker compose logs processor --tail 50
docker compose logs retry --tail 30
docker compose logs ftp_cleanup --tail 30
```

Пересборка и перезапуск после изменения кода:

```bash
docker compose build watcher processor retry ftp_cleanup
docker compose up -d --force-recreate watcher processor retry ftp_cleanup
```

---

## Ключевые переменные окружения (`app.env`)

| Переменная | Сервис | Описание |
|---|---|---|
| `SHADOW_MODE` | processor | `dry_run` / `shadow` / `prod` |
| `SHADOW_TEST_CHAT_ID` | processor | MAX user_id для shadow-режима |
| `MAX_API_BASE` | processor | `https://platform-api.max.ru` |
| `MAX_BOT_TOKEN` | processor | Токен бота |
| `TEST_FILE_REGEX` | watcher, processor | Regex тестовых файлов (default: `\.txt$`) |
| `WATCH_USE_MTIME_CURSOR` | watcher | `1` — сканировать только новые файлы (default: `1`) |
| `WATCH_FILE_STABILIZE_SECONDS` | watcher | Пауза для стабилизации файла (default: `0.2`) |
| `WATCH_LOOP_SLEEP_SECONDS` | watcher | Интервал между сканами (default: `1`) |
| `WATCH_INITIAL_LOOKBACK_SECONDS` | watcher | Окно при старте (default: `300`) |
| `WATCH_MAX_FILE_AGE_SECONDS` | watcher | Максимальный возраст файла (default: `3600`) |
| `FTP_RETENTION_DAYS` | ftp_cleanup | TTL файлов на FTP (default: `7`) |

---

## SQL (диагностика)

```bash
docker exec -it camera-v2-postgres psql -U camera -d camera_v2
```

```sql
SELECT status, count(*) FROM events GROUP BY 1;
SELECT delivery_status, count(*) FROM deliveries GROUP BY 1;
SELECT file_name, status, first_seen_at FROM events ORDER BY first_seen_at DESC LIMIT 20;
```

---

## Миграции на уже поднятой БД

```bash
docker exec -i camera-v2-postgres psql -U camera -d camera_v2 \
  < /opt/cursor-agent/camera-system-v2/migrations/NNN_....sql
```

---

## MAX Bot API — диагностика 401

- Base: **`https://platform-api.max.ru`** (не `botapi.max.ru`)
- Заголовок: **`Authorization: <токен>`** без префикса `Bearer` (`MAX_AUTH_HEADER_STYLE=raw`)
- Получатель: `user_id` или `chat_id` в query-параметре, в теле только `text`/вложения

---

## Риски: совместимость с n8n

- v2 и n8n читают один FTP-каталог → **дублирующие уведомления** для одних и тех же файлов при совпадении получателей.
- FTP cleanup делает **только** v2; n8n при своём сканировании может ожидать уже удалённый файл — согласуйте TTL (`FTP_RETENTION_DAYS`) с ритмом n8n.
- До отключения n8n: если нужен только один канал — деактивируйте workflow в n8n.

---

## Simplified schema (Phase A)

- Таблицы `photo_sources`, `photo_events` созданы миграцией `005_simple_source_event_layers.sql`.
- Watcher делает dual-write (non-critical: ошибка не блокирует основной INSERT в `events`).
- `PIPELINE_SCHEMA_MODE=legacy` — runtime на старом потоке. Phase B/C (переключение runtime) — в плане.
