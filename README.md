# Payment Retry Failure Lab

**A queue acknowledgment is not a payment outcome.**

A reproducible engineering laboratory investigating ambiguous payment
outcomes, duplicate delivery, idempotency, concurrent workers,
transactional outbox recovery, and reconciliation.

The project combines two independently testable layers:

- **Python + SQLite:** 18 deterministic tests using a synthetic payment
  provider, controlled failures, and explicit business-operation state.
- **AWS Emulator 1.0.598:** local integration experiments exercising
  SQS, Lambda, and DynamoDB APIs with pinned execution evidence.

## Validated experiments

| Experiment | What was tested |
|---|---|
| P01 | SQS-triggered Lambda processing, injected failure, and DynamoDB reconciliation |
| P02 | SQS FIFO deduplication and simulated provider-key expiration |
| P03 | Concurrent conditional DynamoDB claims |
| P04 | Transactional outbox recovery and an independently committed synthetic provider |
| P05 | Duplicate and late events without incorrect state regression |

P01 exercises **SQS → Lambda → DynamoDB**.
P02–P05 use host-side Python and AWS CLI against AWS Emulator;
they do not invoke Lambda.

The repository includes source code, repeatable tests, an emulator
image lock, JSON evidence, execution manifests, and SHA-256 checksums.

> **Safety and provenance:** All payment operations, identities,
> amounts, and provider records are synthetic. No live AWS account,
> real customer information, payment credentials, or employer-specific
> implementations are involved. This is an independent engineering
> laboratory, not a production payment processor.

## Reproduce the Python baseline

Requirements: Python 3.10+; standard library only; no AWS account or GPU required.

```bash
python3 -m unittest discover -s tests -v
python3 -m payment_failure_lab.experiments --output reports/results.json
```

`reports/results.json` is generated locally. The committed example in `evidence/results.json` is an output of the scripts against deterministic fixtures and can be regenerated using the command above.

## Core failure

```text
      Message for operation-01
                |
          Worker attempts charge
                |
       Fake provider records charge
                |
         RESPONSE IS LOST
                |
        Message is delivered again
           /              \
  NAIVE: new request      GUARDED: state UNKNOWN
  -> second charge        -> reconcile existing charge
  -> two effects          -> one effect
```

## Experiments

| ID | Injected condition | Intentionally naive behavior | Guarded behavior |
|---|---|---|---|
| P01 | Provider records charge; response is lost; job redelivered | New request causes second synthetic charge | UNKNOWN -> reconciliation; one charge |
| P02 | Dedup windows expire | Same simulated provider key can create another charge after retention | Durable business state prevents duplicate; simulated FIFO dedup also expires |
| P03 | Competing worker claims | Multiple dispatches possible without a durable claim | SQLite conditional claim allows one; parallel claim tested |
| P04 | Crash during publish/ack gap | Operation may be orphaned or duplicate notification emitted | Atomic SQLite outbox; duplicate delivery handled idempotently |
| P05 | Duplicate and late status events | Naive status updates could regress | Pre-verified synthetic events are deduplicated; success is not downgraded |

## Architecture

```text
Business request -> SQLite: operation + outbox (single transaction)
                                     |
                              outbox relay
                                     |
                          synthetic message queue
                                     |
                      guarded worker / conditional claim
                           |                 |
                     new operation     uncertain outcome
                           |                 |
                     fake provider        reconciliation lookup
                           |                 |
                           +--- durable operation state
                                     ^
                             pre-verified events
```

A real implementation on AWS might replace the queue/worker with SQS + Lambda and use a durable DB with explicit concurrency, outbox relay, partial batch failure handling, verified provider webhooks and operational reconciliation. **The Python/SQLite baseline is a simulation. Separate AWS Emulator experiments exercise selected SQS, Lambda, and DynamoDB APIs locally. They do not establish production AWS behavior, real payment-provider guarantees, distributed transaction atomicity, or network-failure resilience.**

### Explicit contracts

1. `operation_id` identifies a **business attempt**, not an SQS message ID. Amount changes for the same operation are rejected.
2. The idempotency key is derived from that operation identity and never regenerated on redelivery.
3. A timeout after a provider call creates an uncertain outcome (`UNKNOWN`), **not a proven failure**.
4. On uncertain outcomes, reconcile; if lookup returns nothing, the lab refuses blind resubmission. This is conservative because provider lookup may not be authoritative.
5. An in-flight operation has a lease. Redelivery before expiry does not invoke the provider; after expiry it goes to reconciliation, not a new charge.
6. Operation creation and outbox insertion are atomic in SQLite. Sending to the queue and marking the outbox row delivered are **not** atomic, so duplicate messages remain possible.
7. Event IDs are deduplicated in SQLite and delayed `PROCESSING` events cannot downgrade `SUCCEEDED`. Real event authenticity is out of scope.

## Understanding the experiments

- The simulated provider idempotency retention is **120 seconds**. This is an invented *test parameter*, **not** any real provider's TTL.
- The simulated FIFO **send-side** dedup window is **300 seconds**, reflecting a documented SQS constraint; it is **not** an SQS emulator and does not model SQS receive semantics.
- The monetary units are arbitrary synthetic integer minor units; no cards or payment accounts exist.
- The test suite exercises two simultaneous SQLite claims, while the P03 CLI report uses sequential competing claims for stable demonstration.
- The webhook examples are marked **pre-verified**. This does not implement signature verification, replay protection, or safe webhook intake.
- The reconciliation interface represents a hypothetical provider lookup; actual providers differ in lookup and consistency guarantees.

See [methodology](docs/methodology.md).

## AWS Emulator follow-up: P01–P05

On 24 November 2026, P01 was also run locally with AWS Emulator 1.0.598 in Docker on Apple Silicon. One synthetic SQS message per mode triggered a Python Lambda function through an event source mapping. The function used DynamoDB for business state and a synthetic charge ledger, then deliberately failed after its first ledger write. The observed retry produced two synthetic charges in the naive mode and one in the guarded mode. Both modes had two Lambda processing attempts; the guarded operation ended `SUCCEEDED`. See [the measured summary](evidence/aws-emulator-p01-2026-11-24.json).

This second run uses a DynamoDB item as the **fake provider ledger**. It does not call a payment provider, reproduce a real network timeout, test production AWS security, or establish cloud production behavior. P02–P05 were subsequently exercised by a separate host-driven SQS/DynamoDB harness, without Lambda.

The recorded image is **ARM64**. The pinned repetition ran on Apple Silicon (Darwin/arm64) with Python 3.15.2 and AWS CLI 2.42.2. Reproduction requires Docker, AWS CLI, the exact image ID in `aws-emulator/image-lock.json`, and an ARM64 host or a separately validated ARM64 emulation setup. The image is not bundled. GitHub Actions on Ubuntu x86-64 runs the deterministic Python tests, privacy scan, evidence hash check, and report comparison; it does **not** rerun AWS Emulator. No x86-64 emulator result is claimed.

To reproduce the AWS Emulator probes on a compatible ARM64 setup:

```bash
python3 aws-emulator/repeat_pinned.py
```

This runner verifies the exact local image ID, version and commit in `aws-emulator/image-lock.json`, starts a fresh container for P01 and another for P02–P05, and stops both after execution. It fails if that image is unavailable; it does not fetch a newer version. On another machine, import the same exported Docker image and verify its ID before running.

Reports include the image identity, tool versions, hashes of the executed source files and per-run outcomes. P02–P05 runs with Python optimization enabled to verify that result checks are not disabled by `-O`.


The P02–P05 runner takes at least five minutes because it checks FIFO deduplication with real elapsed time. It uses host Python and independent AWS CLI processes for SQS and DynamoDB; Lambda is exercised by the separate P01 runner. Its provider-retention check still uses the original fake provider and a manual clock. See [methodology](docs/methodology.md) for the boundaries of each experiment.

The scripts refuse to continue unless `sts:GetCallerIdentity` returns the emulator account `000000000000`. Every AWS CLI call also names the loopback endpoint explicitly. Generated results stay under `reports/` and are excluded from the source package. The Lambda package includes only the emulator's public certificate, not its generated secret. The emulator keeps resources in memory and deletes them when it stops.

The first smoke test exposed a local setup race: `CreateTable` returned `CREATING`, and an immediate write failed. The scripts now wait for `table-exists`. The SQS event source mapping also required the test role to include queue-read permission. AWS warns that [fidelity varies by operation](https://docs.aws.amazon.com/emulator/latest/userguide/fidelity.html) and [security policies are not evaluated as on AWS](https://docs.aws.amazon.com/emulator/latest/userguide/). These results are a local integration observation for the recorded version, not a cloud guarantee.

## Sources

- [AWS Lambda + Amazon SQS: duplicate processing and partial batch responses](https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html)
- [SQS FIFO deduplication interval](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-exactly-once-processing.html)
- [SQS recovery considerations](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/designing-for-outage-recovery-scenarios.html)
- [AWS transactional outbox pattern](https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/transactional-outbox.html)
- [Adyen API idempotency (provider-specific)](https://docs.adyen.com/development-resources/api-idempotency)

## Licensing

Source code is offered under Apache License 2.0; see `LICENSE`. Editorial prose and original diagrams, if added, are © 2026 Rafael Terrero. All rights reserved to the extent applicable. The software license does not automatically license editorial articles or separate graphics. See `CONTENT-LICENSE.md`.

### Historical v0.3 P02–P05 observations

The [reviewed evidence](evidence/aws-emulator-p02-p05-2026-11-24.json) records one successful local run on AWS Emulator 1.0.598. FIFO suppressed a repeat checked at 1.08 seconds and delivered the same deduplication ID again after 305 seconds. One of two concurrent DynamoDB claims won. The outbox test verified transaction rollback and consumed two distinct SQS messages while recording one synthetic ledger charge. Sequential duplicate/late event handling preserved `SUCCEEDED`.

The provider-retention subtest uses the original fake provider with a manual clock, separately from FIFO timing. The historical v0.3 P04 kept its fake ledger in the same DynamoDB transaction as consumer state; this does not model atomicity with an external payment provider. P05 does not test real webhook transport, authentication, or concurrent events. All inputs are synthetic.

### Revised P04 design

The revised P04 provider runs in separate OS processes and commits a private SQLite ledger before responding. DynamoDB transactions contain only local operation/outbox data. The worker persists UNKNOWN before calling the provider and reconciles an uncertain outcome by lookup. Fault cases cover before the provider call, after the provider commit but before local success, and after local success. The provider is synthetic, has immediate authoritative lookup and permanent operation-key idempotency for this run; no real payment or network fault is involved.

The privacy scanner permits only the fixed synthetic container mount `/home/emulator/.aws/emulator`; other personal paths remain findings.

The DEV.to article is maintained separately from this source package. Historical evidence files remain unchanged; new repetition evidence is stored separately.

### v0.4 pinned repetition results

The repeat completed on the exact locked image with two fresh containers. P01 reproduced both the original retry and recovery from failure before UNKNOWN was saved: each guarded path had two attempts, one synthetic charge and SUCCEEDED. P02 identified the correct new FIFO message after 305 seconds. P03 had one winning conditional claim, and P05 preserved SUCCEEDED.

P04 recorded one provider charge for two outbox messages. Before the provider call, the injected failure left UNKNOWN and zero charges; retry required review. After a provider commit but before local success, retry used lookup and reached SUCCEEDED with one charge call. After local success, retry returned ALREADY_SUCCEEDED, again with one charge call.

See `evidence/repeat-pinned-v04/` for the new results, independent provider calls, API trace, execution manifest, and hashed snapshot of executed sources. Historical v0.3 evidence remains unchanged; the editorial draft has been updated to describe the v0.4 repeat. The source ZIP contains no emulator credentials or payment data. The Docker image itself is not bundled; it must match the immutable local image ID in the lock file.
