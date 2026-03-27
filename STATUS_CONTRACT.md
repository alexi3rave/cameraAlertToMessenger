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
