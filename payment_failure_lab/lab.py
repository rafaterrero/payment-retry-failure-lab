"""Payment failure laboratory. All identifiers and payments are synthetic.

Nothing in this module interacts with payment APIs, AWS, or financial accounts.
The provider is a deterministic, intentionally simplified test double.
"""
from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


class LostResponse(Exception):
    """Provider completed a charge, but the response was not received."""


class InjectedFailure(Exception):
    """A deliberate fault at a documented boundary."""


@dataclass
class ManualClock:
    seconds: int = 0

    def advance(self, seconds: int) -> None:
        if seconds < 0:
            raise ValueError("Clock cannot move backward")
        self.seconds += seconds


@dataclass(frozen=True)
class Charge:
    reference: str
    operation_id: str
    amount_minor: int


class FakeProvider:
    """Model a provider with expiring idempotency retention.

    lookup(operation_id) is an illustrative reconciliation capability, not a
    promise that actual payment providers expose equivalent lookup semantics.
    """
    def __init__(self, clock: ManualClock, retention_seconds: int = 120):
        self.clock = clock
        self.retention_seconds = retention_seconds
        self.ledger: list[Charge] = []
        self._key_cache: dict[str, tuple[int, Charge]] = {}
        self.requests = 0

    def charge(self, operation_id: str, amount_minor: int,
               idempotency_key: Optional[str] = None,
               lose_response: bool = False) -> Charge:
        if amount_minor <= 0:
            raise ValueError("Positive synthetic amount required")
        self.requests += 1
        cached = self._key_cache.get(idempotency_key) if idempotency_key else None
        if cached and self.clock.seconds - cached[0] < self.retention_seconds:
            previous = cached[1]
            if (previous.operation_id, previous.amount_minor) != (operation_id, amount_minor):
                raise ValueError("Idempotency key reused with different parameters")
            result = previous
        else:
            result = Charge(f"sim_charge_{len(self.ledger) + 1:04}", operation_id, amount_minor)
            self.ledger.append(result)
            if idempotency_key:
                self._key_cache[idempotency_key] = (self.clock.seconds, result)
        if lose_response:
            raise LostResponse("Response lost after provider-side success")
        return result

    def lookup(self, operation_id: str) -> list[Charge]:
        return [charge for charge in self.ledger if charge.operation_id == operation_id]

    def count(self, operation_id: str) -> int:
        return len(self.lookup(operation_id))


class LocalStore:
    """SQLite state machine; each method uses an independent connection.

    BEGIN IMMEDIATE is sufficient for the controlled single-host SQLite lab.
    It is not a production distributed locking or payment ledger solution.
    """
    def __init__(self, path: Path, clock: ManualClock, lease_seconds: int = 30):
        self.path = str(path)
        self.clock = clock
        self.lease_seconds = lease_seconds
        with closing(self._connection()) as conn:
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS operations (
                operation_id TEXT PRIMARY KEY,
                amount_minor INTEGER NOT NULL CHECK(amount_minor > 0),
                status TEXT NOT NULL,
                idempotency_key TEXT NOT NULL UNIQUE,
                provider_reference TEXT,
                lease_until INTEGER
            );
            CREATE TABLE IF NOT EXISTS outbox (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation_id TEXT NOT NULL REFERENCES operations(operation_id),
                delivered INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS webhook_events (
                event_id TEXT PRIMARY KEY,
                operation_id TEXT NOT NULL
            );
            """)

    def _connection(self):
        conn = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 5000")
        return conn

    def create(self, operation_id: str, amount_minor: int, fail_before_commit: bool = False):
        if not operation_id or amount_minor <= 0:
            raise ValueError("Valid synthetic operation and amount required")
        conn = self._connection()
        try:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT amount_minor FROM operations WHERE operation_id = ?", (operation_id,)
            ).fetchone()
            if existing:
                if existing["amount_minor"] != amount_minor:
                    raise ValueError("Same business operation cannot change amount")
                conn.commit()
                return False
            conn.execute("INSERT INTO operations(operation_id,amount_minor,status,idempotency_key) "
                         "VALUES (?,?,?,?)", (operation_id, amount_minor, "PENDING", "op:" + operation_id))
            conn.execute("INSERT INTO outbox(operation_id) VALUES (?)", (operation_id,))
            if fail_before_commit:
                raise InjectedFailure("Crash before atomic commit")
            conn.commit()
            return True
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def state(self, operation_id: str):
        conn = self._connection()
        try:
            row = conn.execute("SELECT * FROM operations WHERE operation_id=?", (operation_id,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def outbox_pending(self):
        conn = self._connection()
        try:
            return [dict(x) for x in conn.execute(
                "SELECT event_id, operation_id FROM outbox WHERE delivered=0 ORDER BY event_id")]
        finally:
            conn.close()

    def mark_delivered(self, event_id: int):
        with closing(self._connection()) as conn:
            conn.execute("UPDATE outbox SET delivered=1 WHERE event_id=?", (event_id,))

    def claim(self, operation_id: str):
        conn = self._connection()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT status, lease_until FROM operations WHERE operation_id=?", (operation_id,)).fetchone()
            if row is None:
                conn.commit()
                return "UNKNOWN_OPERATION"
            status = row["status"]
            if status == "PENDING":
                conn.execute("UPDATE operations SET status='IN_FLIGHT',lease_until=? WHERE operation_id=?",
                             (self.clock.seconds + self.lease_seconds, operation_id))
                decision = "CALL_PROVIDER"
            elif status == "IN_FLIGHT":
                if self.clock.seconds < row["lease_until"]:
                    decision = "IN_PROGRESS"
                else:
                    conn.execute("UPDATE operations SET status='UNKNOWN',lease_until=NULL WHERE operation_id=?",
                                 (operation_id,))
                    decision = "RECONCILE"
            elif status == "UNKNOWN":
                decision = "RECONCILE"
            elif status == "SUCCEEDED":
                decision = "ALREADY_SUCCEEDED"
            else:
                decision = "MANUAL_REVIEW"
            conn.commit()
            return decision
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def set_unknown(self, operation_id: str):
        with closing(self._connection()) as conn:
            conn.execute("UPDATE operations SET status='UNKNOWN',lease_until=NULL "
                         "WHERE operation_id=? AND status='IN_FLIGHT'", (operation_id,))

    def set_success(self, operation_id: str, provider_reference: str):
        conn = self._connection()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT status,provider_reference FROM operations WHERE operation_id=?",
                               (operation_id,)).fetchone()
            if row is None:
                raise ValueError("Unknown operation")
            if row["status"] == "SUCCEEDED" and row["provider_reference"] != provider_reference:
                raise ValueError("Conflicting provider reference: manual review required")
            if row["status"] == "REVIEW":
                raise ValueError("Cannot silently override review status")
            conn.execute("UPDATE operations SET status='SUCCEEDED',provider_reference=?,lease_until=NULL "
                         "WHERE operation_id=?", (provider_reference, operation_id))
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def set_review(self, operation_id: str):
        with closing(self._connection()) as conn:
            conn.execute("UPDATE operations SET status='REVIEW',lease_until=NULL "
                         "WHERE operation_id=? AND status!='SUCCEEDED'", (operation_id,))

    def apply_event(self, event_id: str, operation_id: str, status: str, provider_reference: str = ""):
        """Accept a PRE-VERIFIED synthetic event, dedupe it, and never downgrade success.

        Real webhook signature verification and provider-state checks are out of scope.
        """
        if status not in {"PROCESSING", "SUCCEEDED"}:
            raise ValueError("Unsupported synthetic webhook status")
        conn = self._connection()
        try:
            conn.execute("BEGIN IMMEDIATE")
            current = conn.execute("SELECT status,provider_reference FROM operations WHERE operation_id=?", (operation_id,)).fetchone()
            if not current:
                raise ValueError("Unknown operation")
            prior_event = conn.execute("SELECT operation_id FROM webhook_events WHERE event_id=?",
                                      (event_id,)).fetchone()
            if prior_event and prior_event["operation_id"] != operation_id:
                raise ValueError("Webhook event ID reused for another operation")
            inserted = conn.execute(
                "INSERT OR IGNORE INTO webhook_events(event_id,operation_id) VALUES(?,?)",
                (event_id, operation_id)
            ).rowcount == 1
            if inserted and status == "SUCCEEDED":
                if not provider_reference:
                    raise ValueError("Verified reference required for success")
                if current["status"] == "SUCCEEDED" and current["provider_reference"] != provider_reference:
                    raise ValueError("Conflicting provider reference: manual review required")
                conn.execute("UPDATE operations SET status='SUCCEEDED',provider_reference=?,lease_until=NULL "
                             "WHERE operation_id=?", (provider_reference, operation_id))
            conn.commit()
            return inserted
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()


class LocalQueue:
    def __init__(self):
        self.messages: list[str] = []

    def send(self, operation_id: str):
        self.messages.append(operation_id)


def relay_outbox(store: LocalStore, queue: LocalQueue, fail_after_send: bool = False,
                 fail_before_send: bool = False) -> int:
    count = 0
    for event in store.outbox_pending():
        if fail_before_send:
            raise InjectedFailure("Crash before queue send")
        queue.send(event["operation_id"])
        count += 1
        if fail_after_send:
            raise InjectedFailure("Crash after queue send, before delivery marker")
        store.mark_delivered(event["event_id"])
    return count


class SimulatedFifoDedup:
    """Send-side 300-second dedup window only; NOT an SQS implementation.

    Does not promise exactly-once consumer-side effects or message delivery.
    """
    def __init__(self, clock: ManualClock, window_seconds: int = 300):
        self.clock = clock
        self.window_seconds = window_seconds
        self.accepted: list[str] = []
        self._seen: dict[str, int] = {}

    def send(self, message_id: str, deduplication_id: str) -> bool:
        last = self._seen.get(deduplication_id)
        if last is not None and self.clock.seconds - last < self.window_seconds:
            return False
        self._seen[deduplication_id] = self.clock.seconds
        self.accepted.append(message_id)
        return True


class NaiveConsumer:
    """Deliberately unsafe: new attempt, new provider key, no business state."""
    def __init__(self, provider: FakeProvider):
        self.provider = provider
        self.attempts = 0

    def handle(self, operation_id: str, amount_minor: int, lose_response: bool = False):
        self.attempts += 1
        try:
            self.provider.charge(operation_id, amount_minor, f"{operation_id}:attempt:{self.attempts}", lose_response)
            return "SUCCEEDED"
        except LostResponse:
            return "RETRY_MESSAGE"


class GuardedConsumer:
    def __init__(self, store: LocalStore, provider: FakeProvider):
        self.store = store
        self.provider = provider

    def handle(self, operation_id: str, lose_response: bool = False):
        decision = self.store.claim(operation_id)
        if decision == "CALL_PROVIDER":
            record = self.store.state(operation_id)
            try:
                result = self.provider.charge(operation_id, record["amount_minor"],
                                              record["idempotency_key"], lose_response)
                self.store.set_success(operation_id, result.reference)
                return "SUCCEEDED"
            except LostResponse:
                self.store.set_unknown(operation_id)
                return "UNKNOWN"
        if decision == "RECONCILE":
            record = self.store.state(operation_id)
            candidates = self.provider.lookup(operation_id)
            if len(candidates) == 1 and candidates[0].amount_minor == record["amount_minor"]:
                self.store.set_success(operation_id, candidates[0].reference)
                return "RECONCILED_SUCCEEDED"
            if len(candidates) > 1:
                self.store.set_review(operation_id)
                return "MANUAL_REVIEW_DUPLICATE"
            # 'No charge found' is not proof of no charge unless lookup is authoritative.
            return "UNKNOWN_REQUIRES_REVIEW"
        return decision
