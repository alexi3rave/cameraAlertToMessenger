# Runbook: camera-system-v2 (параллельно с n8n)

## Контуры

- **Старый:** камера → FTP → n8n → MAX (`camera-n8n`, данные workflow).
- **Новый v2:** камера → FTP → watcher → events → processor → deliveries → MAX (`camera-v2-*`).

Оба могут читать один каталог FTP на хосте; v2 **не отключает** n8n.

## Каталоги

- Compose: `camera-system-v2/compose/`
- Конфигурация: `camera-system-v2/env/app.env` (docker compose читает **этот** файл; секреты держите здесь с `chmod 600`).
- FTP на хосте (пример): `/home/poe3/ftp` → в контейнерах `/source`.

## Основные команды

Все команды — из каталога `compose`:

```bash
cd /opt/cursor-agent/camera-system-v2/compose
docker compose ps
docker compose logs watcher --tail 50
docker compose logs processor --tail 50
docker compose logs retry --tail 30
docker compose logs ftp_cleanup --tail 30
```

Пересборка после изменения кода:

```bash
docker compose build watcher processor retry ftp_cleanup
docker compose up -d --force-recreate watcher processor retry ftp_cleanup
```

## Проверка здоровья (скрипт)

```bash
chmod +x /opt/cursor-agent/camera-system-v2/scripts/v2_healthcheck.sh
/opt/cursor-agent/camera-system-v2/scripts/v2_healthcheck.sh
```

## SQL (диагностика)

```bash
docker exec -it camera-v2-postgres psql -U camera -d camera_v2
```

Примеры:

```sql
SELECT status, count(*) FROM events GROUP BY 1;
SELECT delivery_status, count(*) FROM deliveries GROUP BY 1;
SELECT id, status, first_seen_at, ftp_removed_at, left(full_path, 80) FROM events ORDER BY first_seen_at DESC LIMIT 20;
```

## Миграции на уже поднятой БД

Файлы в `migrations/` подхватываются только при **первом** создании тома Postgres. Для существующей БД:

```bash
docker exec -i camera-v2-postgres psql -U camera -d camera_v2 < /opt/cursor-agent/camera-system-v2/migrations/NNN_....sql
```

## MAX Bot API (401 / не уходит сообщение)

Официально: [POST /messages](https://dev.max.ru/docs-api/methods/POST/messages).

- База обычно **`https://platform-api.max.ru`**, не `botapi.max.ru`.
- Заголовок **`Authorization: <токен>`** — токен **без** префикса `Bearer` (в коде по умолчанию `MAX_AUTH_HEADER_STYLE=raw`). Если n8n шлёт с Bearer — в `app.env`: `MAX_AUTH_HEADER_STYLE=bearer`.
- Получатель: **`chat_id` или `user_id` в query**, в теле JSON только **`text`** (и вложения при необходимости).

После правки `app.env` и кода: `docker compose build processor && docker compose up -d --force-recreate processor`.

## Ограничения текущей версии

- Нет UI управления; правки маршрутов и получателей — через SQL или клиент к БД.
- Режим `prod` в processor: убедитесь, что MAX API и токены заданы в `app.env`.
- Очистка FTP: сервис `ftp_cleanup`, интервал и TTL — переменные `FTP_*` в `app.env`.

## Риски совместимости с n8n

- Удаление файлов старше 7 дней делает **только** v2 для путей, попавших в `events` в терминальных статусах; n8n при своём сканировании может ещё ожидать файл — при совпадении сценариев согласуйте TTL и порядок обработки.


## Simplified schema rollout (Phase A)

- Новые таблицы `photo_sources`, `photo_events`, `photo_event_attempts` добавляются миграцией `005_simple_source_event_layers.sql` без переключения runtime.
- Сервисы пока работают в legacy-потоке; переменная `PIPELINE_SCHEMA_MODE` пока только логируется (`legacy` по умолчанию).
- Перед применением миграции на прод-контуре согласуйте окно и сделайте snapshot/backup БД.
