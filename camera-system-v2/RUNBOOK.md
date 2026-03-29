# camera-system-v2 — операционная шпаргалка

Полная документация — в корне репозитория (`repo-cameras/*.md`):
`project.md`, `ARCHITECTURE.md`, `DB model.md`, `RULES.md`, `SERVER_STATUS_*.md`, `CURRENT STATE.md`

---

## Быстрые команды

```bash
# из /opt/cursor-agent/camera-system-v2/compose

docker compose ps
docker compose logs watcher --tail 50
docker compose logs processor --tail 50

# пересборка
docker compose build watcher processor retry ftp_cleanup
docker compose up -d --force-recreate watcher processor retry ftp_cleanup

# БД
docker exec -it camera-v2-postgres psql -U camera -d camera_v2
```

```sql
SELECT status, count(*) FROM events GROUP BY 1;
SELECT delivery_status, count(*) FROM deliveries GROUP BY 1;
SELECT file_name, status, first_seen_at FROM events ORDER BY first_seen_at DESC LIMIT 20;
```

## Миграция на существующей БД

```bash
docker exec -i camera-v2-postgres psql -U camera -d camera_v2 < migrations/NNN_....sql
```
