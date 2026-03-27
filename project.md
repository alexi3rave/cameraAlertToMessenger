PROJECT: camera-system-v2

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

решение по roadmap:

Stage 4 с UI