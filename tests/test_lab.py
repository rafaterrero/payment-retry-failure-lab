import tempfile
import threading
import unittest
from pathlib import Path
from payment_failure_lab.lab import (ManualClock, FakeProvider, LocalStore, LocalQueue, SimulatedFifoDedup,
  NaiveConsumer, GuardedConsumer, LostResponse, InjectedFailure, relay_outbox)
from payment_failure_lab.experiments import run

class PaymentFailureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.clock = ManualClock()
        self.provider = FakeProvider(self.clock, retention_seconds=120)
        self.store = LocalStore(Path(self.temp.name) / "state.sqlite", self.clock)
        self.guard = GuardedConsumer(self.store, self.provider)

    def test_p01_naive_lost_response_creates_duplicate(self):
        n = NaiveConsumer(self.provider)
        self.assertEqual(n.handle("op-01", 1000, lose_response=True), "RETRY_MESSAGE")
        self.assertEqual(n.handle("op-01", 1000), "SUCCEEDED")
        self.assertEqual(self.provider.count("op-01"), 2)

    def test_p01_guarded_lost_response_reconciles_without_new_charge(self):
        self.store.create("op-01", 1000)
        self.assertEqual(self.guard.handle("op-01", lose_response=True), "UNKNOWN")
        self.assertEqual(self.store.state("op-01")["status"], "UNKNOWN")
        self.assertEqual(self.guard.handle("op-01"), "RECONCILED_SUCCEEDED")
        self.assertEqual(self.provider.count("op-01"), 1)

    def test_p02_stable_key_not_permanent_after_provider_ttl(self):
        self.provider.charge("op-01", 1000, "stable")
        self.provider.charge("op-01", 1000, "stable")
        self.assertEqual(self.provider.count("op-01"), 1)
        self.clock.advance(121)
        self.provider.charge("op-01", 1000, "stable")
        self.assertEqual(self.provider.count("op-01"), 2)

    def test_p02_fifo_send_dedup_window_is_not_indefinite(self):
        fifo = SimulatedFifoDedup(self.clock)
        self.assertTrue(fifo.send("m-1", "same-id"))
        self.clock.advance(120)
        self.assertFalse(fifo.send("m-1", "same-id"))
        self.clock.advance(181)
        self.assertTrue(fifo.send("m-1", "same-id"))
        self.assertEqual(len(fifo.accepted), 2)

    def test_p02_guarded_replay_after_ttl(self):
        self.store.create("op-01", 1000)
        self.assertEqual(self.guard.handle("op-01"), "SUCCEEDED")
        self.clock.advance(121)
        self.assertEqual(self.guard.handle("op-01"), "ALREADY_SUCCEEDED")
        self.assertEqual(self.provider.count("op-01"), 1)

    def test_unknown_without_authoritative_confirmation_no_blind_retry(self):
        self.store.create("op-01", 1000)
        self.assertEqual(self.store.claim("op-01"), "CALL_PROVIDER")
        self.clock.advance(31)
        self.assertEqual(self.guard.handle("op-01"), "UNKNOWN_REQUIRES_REVIEW")
        self.assertEqual(self.provider.count("op-01"), 0)
        self.assertEqual(self.store.state("op-01")["status"], "UNKNOWN")

    def test_p03_competing_claims_reject_second(self):
        self.store.create("op-01", 1000)
        self.assertEqual(self.store.claim("op-01"), "CALL_PROVIDER")
        self.assertEqual(self.store.claim("op-01"), "IN_PROGRESS")
        self.assertEqual(self.provider.count("op-01"), 0)

    def test_p03_two_threads_claim_only_one(self):
        self.store.create("op-01", 1000)
        start = threading.Barrier(3)
        outcomes = []
        def claim():
            start.wait()
            outcomes.append(self.store.claim("op-01"))
        one = threading.Thread(target=claim)
        two = threading.Thread(target=claim)
        one.start(); two.start(); start.wait()
        one.join(); two.join()
        self.assertCountEqual(outcomes, ["CALL_PROVIDER", "IN_PROGRESS"])

    def test_expired_lease_does_not_issue_another_charge(self):
        self.store.create("op-01", 1000)
        self.assertEqual(self.store.claim("op-01"), "CALL_PROVIDER")
        self.clock.advance(31)
        self.assertEqual(self.guard.handle("op-01"), "UNKNOWN_REQUIRES_REVIEW")
        self.assertEqual(self.provider.requests, 0)

    def test_p04_atomic_outbox_rollback(self):
        with self.assertRaises(InjectedFailure):
            self.store.create("op-01", 1000, fail_before_commit=True)
        self.assertIsNone(self.store.state("op-01"))
        self.assertEqual(self.store.outbox_pending(), [])

    def test_p04_outbox_recovers_crash_before_publish(self):
        self.store.create("op-01", 1000)
        queue = LocalQueue()
        with self.assertRaises(InjectedFailure):
            relay_outbox(self.store, queue, fail_before_send=True)
        self.assertEqual(len(queue.messages), 0)
        self.assertEqual(len(self.store.outbox_pending()), 1)
        self.assertEqual(relay_outbox(self.store, queue), 1)
        self.assertEqual(queue.messages, ["op-01"])

    def test_p04_duplicate_publish_replayed_without_duplicate_charge(self):
        self.store.create("op-01", 1000)
        queue = LocalQueue()
        with self.assertRaises(InjectedFailure):
            relay_outbox(self.store, queue, fail_after_send=True)
        relay_outbox(self.store, queue)
        self.assertEqual(len(queue.messages), 2)
        self.assertEqual([self.guard.handle(m) for m in queue.messages],
                         ["SUCCEEDED", "ALREADY_SUCCEEDED"])
        self.assertEqual(self.provider.count("op-01"), 1)

    def test_p05_webhook_duplicate_and_late_event_do_not_downgrade(self):
        self.store.create("op-01", 1000)
        self.assertTrue(self.store.apply_event("evt-1", "op-01", "SUCCEEDED", "sim_charge"))
        self.assertFalse(self.store.apply_event("evt-1", "op-01", "SUCCEEDED", "sim_charge"))
        self.assertTrue(self.store.apply_event("evt-2", "op-01", "PROCESSING"))
        self.assertEqual(self.store.state("op-01")["status"], "SUCCEEDED")

    def test_webhook_event_id_cannot_move_to_other_operation(self):
        self.store.create("op-01", 1000)
        self.store.create("op-02", 1000)
        self.assertTrue(self.store.apply_event("evt-1", "op-01", "PROCESSING"))
        with self.assertRaises(ValueError):
            self.store.apply_event("evt-1", "op-02", "PROCESSING")
        self.assertEqual(self.store.state("op-02")["status"], "PENDING")

    def test_conflicting_provider_reference_does_not_overwrite_success(self):
        self.store.create("op-01", 1000)
        self.assertTrue(self.store.apply_event("evt-a", "op-01", "SUCCEEDED", "sim_charge_a"))
        with self.assertRaises(ValueError):
            self.store.apply_event("evt-b", "op-01", "SUCCEEDED", "sim_charge_b")
        self.assertEqual(self.store.state("op-01")["provider_reference"], "sim_charge_a")

    def test_same_operation_cannot_change_amount(self):
        self.store.create("op-01", 1000)
        with self.assertRaises(ValueError):
            self.store.create("op-01", 2000)

    def test_provider_rejects_same_key_different_params(self):
        self.provider.charge("op-01", 1000, "key")
        with self.assertRaises(ValueError):
            self.provider.charge("op-01", 2000, "key")

    def test_report_runs_all_five(self):
        output = run()
        self.assertEqual(len(output["scenarios"]), 5)
        p1 = output["scenarios"]["P01_lost_ack"]
        self.assertEqual(p1["naive"]["charges"], 2)
        self.assertEqual(p1["guarded"]["charges"], 1)
        p2 = output["scenarios"]["P02_key_expiry"]
        self.assertEqual((p2["naive"]["charges"], p2["guarded"]["charges"]), (2, 1))
        p3 = output["scenarios"]["P03_claim_race"]
        self.assertEqual(p3["second_claim"], "IN_PROGRESS")
        p4 = output["scenarios"]["P04_outbox_delivery"]
        self.assertEqual(p4["charges"], 1)
        p5 = output["scenarios"]["P05_event_order"]
        self.assertEqual(p5["final_state"], "SUCCEEDED")

if __name__ == "__main__":
    unittest.main()
