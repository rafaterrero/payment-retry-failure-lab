"""Controlled scenarios; only report observed results, not expected assertions."""
import argparse
import json
import tempfile
from pathlib import Path
from .lab import (ManualClock, FakeProvider, LocalStore, LocalQueue, SimulatedFifoDedup,
                  NaiveConsumer, GuardedConsumer, LostResponse, InjectedFailure, relay_outbox)


def environment(root: Path):
    clock = ManualClock()
    provider = FakeProvider(clock, retention_seconds=120)
    store = LocalStore(root / "state.sqlite", clock, lease_seconds=30)
    return clock, provider, store


def run():
    results = {"project": "Payment Retry Failure Lab", "data": "synthetic",
               "aws_or_payment_api_calls": 0, "scenarios": {}}
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        clock, provider, store = environment(root)
        # P01 - same business intent; ambiguous response then redelivery.
        naive = NaiveConsumer(provider)
        before = naive.handle("op-naive-01", 1200, lose_response=True)
        after = naive.handle("op-naive-01", 1200)
        naive_count = provider.count("op-naive-01")
        store.create("op-safe-01", 1200)
        guard = GuardedConsumer(store, provider)
        first = guard.handle("op-safe-01", lose_response=True)
        second = guard.handle("op-safe-01")
        results["scenarios"]["P01_lost_ack"] = {
            "naive": {"first": before, "redelivery": after, "charges": naive_count},
            "guarded": {"first": first, "redelivery": second,
                        "charges": provider.count("op-safe-01"), "state": store.state("op-safe-01")["status"]}}
    with tempfile.TemporaryDirectory() as temp:
        clock, provider, store = environment(Path(temp))
        stable_key = "same-operation-key"
        provider.charge("op-expired-naive", 1200, stable_key)
        clock.advance(121)
        provider.charge("op-expired-naive", 1200, stable_key)
        store.create("op-expired-safe", 1200)
        guard = GuardedConsumer(store, provider)
        first = guard.handle("op-expired-safe")
        clock.advance(121)
        replay = guard.handle("op-expired-safe")
        fifo_clock = ManualClock()
        fifo = SimulatedFifoDedup(fifo_clock)
        sends = [fifo.send("msg-1", "dedup:op-1")]
        fifo_clock.advance(120)
        sends.append(fifo.send("msg-1", "dedup:op-1"))
        fifo_clock.advance(181)
        sends.append(fifo.send("msg-1", "dedup:op-1"))
        results["scenarios"]["P02_key_expiry"] = {
            "simulated_provider_cache_ttl_seconds": 120, "provider_replay_after_seconds": 121,
            "simulated_fifo_dedup_window_seconds": 300,
            "fifo_sends_accepted": sends,
            "note": "Two separate synthetic expiry windows; neither models a provider or SQS service end-to-end",
            "naive": {"charges": provider.count("op-expired-naive")},
            "guarded": {"first": first, "replay": replay, "charges": provider.count("op-expired-safe")}}
    with tempfile.TemporaryDirectory() as temp:
        clock, provider, store = environment(Path(temp))
        store.create("op-workers", 1200)
        first = store.claim("op-workers")
        second = store.claim("op-workers")
        # The first worker now makes the side-effect; the second was rejected by state.
        charged = provider.charge("op-workers", 1200, "op:op-workers")
        store.set_success("op-workers", charged.reference)
        results["scenarios"]["P03_claim_race"] = {"first_claim": first, "second_claim": second,
                "charges": provider.count("op-workers"), "state": store.state("op-workers")["status"],
                "note": "sequential competing claims; SQLite atomicity separately covered by tests"}
    with tempfile.TemporaryDirectory() as temp:
        clock, provider, store = environment(Path(temp))
        store.create("op-outbox", 1200)
        q = LocalQueue()
        try:
            relay_outbox(store, q, fail_after_send=True)
        except InjectedFailure:
            pass
        after_crash = len(q.messages)
        relay_outbox(store, q)
        consumer = GuardedConsumer(store, provider)
        outcomes = [consumer.handle(operation_id) for operation_id in q.messages]
        results["scenarios"]["P04_outbox_delivery"] = {"messages_after_crash": after_crash,
                    "messages_after_recovery": len(q.messages), "outcomes": outcomes,
                    "charges": provider.count("op-outbox"), "pending_outbox": len(store.outbox_pending())}
    with tempfile.TemporaryDirectory() as temp:
        clock, provider, store = environment(Path(temp))
        store.create("op-webhook", 1200)
        events = [store.apply_event("evt-success", "op-webhook", "SUCCEEDED", "sim_charge_webhook"),
                  store.apply_event("evt-success", "op-webhook", "SUCCEEDED", "sim_charge_webhook"),
                  store.apply_event("evt-late", "op-webhook", "PROCESSING")]
        results["scenarios"]["P05_event_order"] = {"event_new_flags": events,
                         "final_state": store.state("op-webhook")["status"]}
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="reports/results.json")
    args = parser.parse_args()
    data = run()
    path = Path(args.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(data, indent=2))
    print(f"Evidence written: {path}")


if __name__ == "__main__":
    main()
