# Authorization ownership and upgrade

The supplied processor is synthetic. These safeguards protect its durable authorization ledger;
they are not a claim of integration with a real payment network.

## Protocol

`authorization_decision` binds each code permanently to one order. It has three states:
`UNDECIDED`, `CLAIMED`, and `RETIRED`. Retirement is irreversible. The current processor generates
globally unique codes. An adapter whose codes are only unique within a provider/account must add
that namespace to the decision key before use. Do not reuse codes or order identifiers.

Approval and reconciliation use one short database transaction per code, in decision → payment →
outbox order. An existing decision is locked for update before reading fresh payment state.
A missing row is inserted and flushed; a racing insert fails its entire transaction, then retries
from outside the rollback boundary, at most five attempts. There is no nested `REQUIRES_NEW`
transaction and no continuation inside an aborted PostgreSQL transaction.

Claim, payment authorization and completion event commit together. The payment's optimistic version
still arbitrates concurrent different-code approvals and cancellation; a losing transaction leaves
no claim or completion event. An approval of a retired code leaves an open payment retryable and
emits no completion. Cross-order reuse fails closed.

Reconciliation locks the same decision, then reads the payment. It recognizes an authorized
payment's kept code, including a pre-upgrade payment with no decision yet. Otherwise it commits
retirement before calling the provider. No provider operation runs while this decision lock is
held. Failed releases and process death after retirement are retried from subsequent outstanding
hold listings. The existing fast-release path only acts on terminal payments and keeps the
authorized code; terminal payments never become open again.

The grace period is a courtesy window for in-flight attempts, not proof an old callback cannot
arrive. Grace plus one scheduled interval is not a hard cleanup deadline. Provider/database
availability, scheduling delays, backlog and pagination affect actual time to release. Monitor
`ledgermesh.payments.unkept.oldest.seconds` and release failures. Never delete retirement rows to
clear a backlog, and never repair a failed release by reclaiming its code.

## Stop/drain upgrade (not rolling)

### Inventory prerequisite for 5.0.0

The inventory upgrade has a separate limitation: 5.0.0 deducted stock without reservation rows.
The new ledger cannot reconstruct those deductions; a release with no reservation credits zero
and leaves a released marker. Before upgrading inventory, stop new intake, let open orders reach
terminal states, and verify their compensations finished on the old inventory service. Terminal
order status alone is not proof that a queued release has been applied.

If inventory was already upgraded with old holds outstanding, reconcile those deductions from
audited pre-upgrade records before adjusting stock. Do not invent reservation quantities or replay
an order payload as a stock credit: that can double-credit an already compensated order. The payment
compensation resend endpoint does not backfill the missing inventory history.

### Payment activation

Old binaries do not consult the decision table and can accept retired authorizations. Adding the
table alone cannot protect a mixed deployment.

1. Pause payment consumers and sweeps on **every old instance**. Stop admitting new processor calls.
2. Drain outstanding calls, late-callback tasks and database transactions, then stop every old
   payment process. If a process must be killed, prove it is stopped before proceeding; outstanding
   provider holds can be reconciled by the new version. Do not leave a callback-capable old process.
3. Back up the payment database. Retain existing payments, outbox rows and processor holds.
4. Add the table below, or let the existing `spring.jpa.hibernate.ddl-auto=update` application
   startup add it. This repository does not currently use a migration framework. Do not use
   `create` or `create-drop` outside tests.
5. Start only the new payment binary. Verify existing authorized payments retain their own codes,
   open payments retry, cancellation compensation proceeds, and orphan/unkept holds retire.
6. Resume consumers and sweeps, and monitor the processor ledger and release failures.

If rollback is necessary, stop/drain the new processes first. An old binary is **not a safe rollback
target** while retired codes or old callback deliveries can still appear. Preserve the table and
resolve that exposure before any rollback; prefer fixing forward. Never run old and new binaries
concurrently to avoid downtime.

## Additive PostgreSQL 16 DDL

Run against the payment database after the stop/drain step. The table intentionally has no payment
foreign key, because absent payments also need retirement tombstones. The primary key supports
the per-code lookup and lock; no secondary index is needed by this protocol.

```sql
CREATE TABLE authorization_decision (
    authorization_code varchar(64) PRIMARY KEY,
    order_id varchar(36) NOT NULL,
    state varchar(16) NOT NULL
        CHECK (state IN ('UNDECIDED', 'CLAIMED', 'RETIRED')),
    version bigint NOT NULL
);
```

If the table already exists, verify its columns and retained decisions; do not replace it.
The PostgreSQL integration contract boots the service over an old payment schema with an existing
authorized row, checks the additive upgrade, and runs the same race tests used with H2.

```sh
mvn -pl payment-service -am verify -Dit.test=AuthorizationOwnershipPostgresIT \
  -Dfailsafe.failIfNoSpecifiedTests=false
```

For transaction semantics, see [PostgreSQL row locks](https://www.postgresql.org/docs/16/explicit-locking.html#LOCKING-ROWS)
and [Spring programmatic transactions](https://docs.spring.io/spring-framework/reference/data-access/transaction/programmatic.html).
