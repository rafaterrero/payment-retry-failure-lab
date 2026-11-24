# Experimental methodology and limits

## Aim

Disprove the assumption that successful queue delivery/retry and a provider key necessarily imply exactly one financial effect. Compare deliberately naive behavior against explicit application state and reconciliation.

## Design

- Controlled local Python test doubles; deterministic manual clocks; synthetic IDs and amounts.
- SQLite state transitions use `BEGIN IMMEDIATE` to enforce local conditional ownership of a business operation; concurrent claims tested via two threads and independent DB connections.
- Simulated provider returns a charge, or **records it then loses the response**. Its key dedup cache lasts 120 simulated seconds. This is an invented provider behavior for the laboratory.
- The separate simulated FIFO send-side dedup window lasts 300 simulated seconds. It does not emulate Amazon SQS and cannot prove SQS ordering or exact-once processing.
- Outbox creates state+event in one SQLite transaction. Relay can crash after queue send and before local delivery marker, proving that duplicate delivery remains possible.
- The synthetic webhook handler assumes events have already been verified upstream. No public HTTP endpoint is built.

## Evidence levels

**Observed in this lab:** counts of synthetic provider charges, messages, claims, state transitions, event acceptance. Reproduce with `python3 -m payment_failure_lab.experiments`.

**Not observed:** live payment results; real Lambda/SQS behavior; actual PSP contract, guarantee of provider querying by merchant reference; financial reconciliation against bank ledger; production availability; PCI compliance; multi-region failover.

## AWS Emulator extension

The optional `aws-emulator/` scripts call only `https://127.0.0.1:9297`, verify the emulator account before creating resources, and use synthetic inputs. P01 uses an SQS event source mapping and Lambda. The P02–P05 runner uses host Python orchestration and AWS CLI processes; it does not invoke Lambda.

- P02 measures the emulator's FIFO send-deduplication window with elapsed wall time. Provider retention is still a separate 120-second manual-clock test double; a DynamoDB operation record controls whether the guarded path calls that double again.
- P03 starts two independent CLI requests together using a thread barrier. They contend on one DynamoDB conditional update. This tests one local claim race, not a distributed Lambda fleet.
- P04 in v0.3 committed its fake ledger and local success together in DynamoDB. The revised harness retains the operation/outbox transaction and rollback check but moves the provider ledger to independent SQLite processes. The local worker commits UNKNOWN before calling the provider. Injected exceptions occur before the provider call, after its commit but before local success, and after local success. Lookup reconciles the uncertain charged operation; no-result lookup leaves UNKNOWN for review without another charge call. Exceptions are controlled faults, not OS kills or real network failures.
- P05 processes three pre-verified synthetic events sequentially. Event deduplication and the conditional state transition are stored transactionally. Webhook authentication, transport and concurrent event races are outside this run.

Each run writes an aggregate `results.json` and a request/response trace under a unique `reports/` directory. Only reviewed aggregate evidence is included in distributable archives. A failed run records its error and cannot count as a passed scenario.

## Correct interpretation of the extension

A successful experiment demonstrates a specific invariant *within this model*. It cannot prove the absence of all duplicates under arbitrary network partitions or across providers. A conservative `UNKNOWN` outcome may require manual operational intervention; that is an intentional safety/availability trade-off.

## Review before publishing

- Confirm links to official primary sources.
- Run `python3 -m unittest discover -s tests -v` and `python3 -m payment_failure_lab.experiments`.
- Run `python3 scripts/privacy_check.py` and manual review. The scan is not a full secret audit.
- Do not include machine names, personal filesystem paths, organization/customer internals, infrastructure identifiers, API keys, or real payment data.
- Do not claim these experiments were performed as part of a commercial deployment.

## Pinned repetition

The revised repeat runner starts separate fresh containers for P01 and P02–P05 using the immutable image identity in `aws-emulator/image-lock.json`. It checks the emulator version/commit, records CLI and Python versions and source hashes, and stops each container after execution. P01 also tests a failure before UNKNOWN is persisted and recovery after the IN_FLIGHT lease expires. P02 compares the final received MessageId to the final SendMessage result and requires it to differ from the first message. Runtime checks in P02–P05 remain active under Python optimization.

The fake provider has immediate authoritative lookup and indefinite per-operation deduplication for P04. These are declared test contracts, not payment-provider guarantees. The host worker and its before/after-result exceptions remain a sequential controlled model; concurrency and network partitions at that boundary are not established by this run.
