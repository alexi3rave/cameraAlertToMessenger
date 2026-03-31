STATUS CONTRACT

source of truth: PostgreSQL

---

EVENTS

discovered
ready
processing
sent
failed_retryable
quarantine

---

DELIVERIES

pending
started
sent
failed

---

rules

pending — единственный начальный статус delivery
uploaded не используется как основной статус
retry работает через events.failed_retryable + next_retry_at
исчерпание retry переводит событие в quarantine

watcher runtime:
- новые валидные события создаются как ready
- слишком старые файлы не теряются: создаются как quarantine с reason=too_old

reaper runtime:
- stale processing (по locked_at) переводится обратно в failed_retryable
