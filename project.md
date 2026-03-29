PROJECT: camera-system-v2

---

структура документации (repo-cameras/):

project.md               — описание проекта, масштаб, roadmap, текущий статус
ARCHITECTURE.md          — архитектура системы, компоненты, принципы
DB model.md              — модель данных, таблицы, статусные контракты
RULES .md                — правила разработки, ограничения, env-дисциплина
SERVER_STATUS_*.md       — состояние сервера, окружение, операционные команды
CURRENT STATE .md        — актуальный статус реализации, что сделано / что осталось
STATUS_CONTRACT.md       — зафиксированные контракты статусов events и deliveries

репо сервисов:
camera-system-v2/RUNBOOK.md  — быстрые операционные команды (шпаргалка)

---

тип:
объектная система обработки событий с камер с отправкой уведомлений в MAX

масштаб:
20–25 объектов
200–250 камер
100–150 получателей

архитектурный принцип:

строгая маршрутизация:

camera → site → recipients

новый контур работает параллельно с n8n:

legacy pipeline:
camera → FTP → n8n → MAX

v2 pipeline:
camera → FTP → watcher → event registry → processor → deliveries → MAX

v2 НЕ заменяет n8n на текущем этапе.

текущий статус (обновлено: 2026-03-29):

Stage 3 — ЗАКРЫТ 
текущая цель — Stage 4 (UI)

✅ отправка фото в MAX в v2 успешно реализована и работает end-to-end
✅ watcher стабильно создает события (ready)
✅ processor отправляет фото + текст и завершает события в sent
✅ выровнены контракты статусов в БД:
   events = discovered/ready/processing/sent/failed_retryable/quarantine
   deliveries = pending/started/sent/failed
✅ MAX upload переведен на актуальный flow (/uploads?type=image)
✅ добавлено время события в текст сообщения (без имени файла)
✅ v2 переведен на единый источник FTP через `FTP_SOURCE_PATH` (runtime: `/home/alex/camera-system/ftp-uploads`)
✅ n8n и `camera-ftp` работают в общей docker-сети `camera-infra`, host для n8n: `camera-ftp`
✅ время в уведомлениях v2 отображается в часовом поясе `Europe/Moscow` (MSK)
✅ watcher оптимизирован: mtime-курсор, seen_keys+seen_names дедуп, latency <1 сек (было ~3 мин)
✅ тестовые файлы (.txt): отдельное text-only уведомление «✅ Test event received»
✅ исправлены дубли уведомлений (FTP mtime update больше не создает повторный event)

цель Stage 3:

гарантировать надежный lifecycle событий:

retry
dedup
routing
auto onboarding
ftp lifecycle TTL 7 дней

ключевые требования:

каждое фото должно быть:
доставлено
доставлено только нужному получателю
доставлено 1 раз

система должна:

переживать restart контейнеров
не терять события
не требовать ручного SQL onboarding
не требовать ручного контроля FTP

ограничения:

один сервер
docker compose
postgres
без kafka
без UI на Stage 1-3
без сложной multi-tenant логики

предстоящие задачи:

1) наблюдаемость и контроль качества доставки
   - дашборд по sent/failed_retryable/quarantine
   - алерты при росте failed_retryable и 401/5xx от MAX

2) эксплуатационная устойчивость
   - автоматическая проверка валидности MAX токена (health-check)
   - регламент безопасной ротации токенов без простоя

3) оптимизация и верификация latency
   - регулярный контроль метрик trigger→first_seen и first_seen→sent
   - тюнинг watcher/processor параметров под продовую нагрузку

4) Stage 4
   - запуск UI для маршрутизации, статусов и ручных действий по quarantine

5) конфигурационная дисциплина n8n
   - экспортировать актуальные workflow/credentials-mapping (без секретов) в репозиторий
   - вести versioning изменений n8n рядом с инфраструктурной документацией

решение по roadmap:

Stage 4 с UI