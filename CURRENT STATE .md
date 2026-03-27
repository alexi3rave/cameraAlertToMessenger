CURRENT STATE

Stage 3 partially complete.

контур v2 работает параллельно с n8n.

развернутые сервисы:

camera-v2-postgres
camera-v2-watcher
camera-v2-processor
camera-v2-retry
camera-v2-ftp-cleanup
camera-ftp
camera-n8n

repo:

/opt/cursor-agent/camera-system-v2

---

реализовано:

events registry
deliveries registry

watcher:

обнаружение файлов
извлечение camera_code
извлечение site из пути
регистрация event

processor:

создание deliveries
отправка в MAX

retry_worker:

обработка failed_retryable

auto onboarding:

новая камера создается автоматически
новый site создается автоматически

dedup:

camera_code + file_name + size + mtime

поддержка вложенных FTP папок

single env файл:

env/app.env

---

выявленные расхождения:

1. статус deliveries:

в документах ранее встречался uploaded
целевой контракт: pending / started / sent / failed

решение:

используем pending как единственный начальный статус
uploaded не используем как основной

2. lifecycle events описан по-разному в разных документах

зафиксирован единый контракт:

discovered
ready
processing
sent
failed_retryable
quarantine

3. ftp cleanup сервис существует, но требуется подтвердить:

TTL = 7 дней
ftp_removed_at корректно заполняется

4. доступ к env/app.env ограничен правами

это мешает runtime аудиту:

retry backoff
TTL
SHADOW_MODE

---

текущая цель:

закрыть Stage 3 полностью

---

Stage 3 definition of done:

retry

failed_retryable события повторно обрабатываются

ftp cleanup

файлы старше 7 дней удаляются
events.ftp_removed_at заполняется

routing

невозможно отправить фото не тому получателю

dedup

повторные файлы не создают новые deliveries

auto onboarding

новая камера начинает отправку без ручного SQL

устойчивость

restart контейнеров не ломает pipeline

n8n не затронут

---

NEXT ITERATION PLAN

шаг 1

зафиксировать STATUS CONTRACT

единый список статусов events
единый список статусов deliveries

результат:

STATUS_CONTRACT.md

---

шаг 2

runtime аудит

SQL:

распределение events.status
распределение deliveries.status
failed_retryable count
quarantine count
ftp_removed_at заполнение

логи:

watcher
processor
retry
ftp_cleanup

результат:

таблица

document vs runtime

---

шаг 3

закрыть Stage 3:

retry подтвержден логами

ftp cleanup подтвержден SQL

routing проверен на тестовой камере

dedup подтвержден повторной загрузкой файла

auto onboarding подтвержден новой камерой

---

шаг 4

синхронизировать документацию:

CURRENT STATE
Stage reports

убрать противоречия

---

после закрытия Stage 3:

решение:

Stage 4 с UI