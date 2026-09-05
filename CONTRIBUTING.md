# Contributing

## Prerequisites

* JDK 21 and Maven 3.9
* Docker with the Compose plugin (integration tests use Testcontainers)
* Python 3 for the chaos harness

## Workflow

```bash
make lint      # spotless check (google-java-format)
make format    # apply formatting
make test      # unit tests + Testcontainers integration tests
make up        # build images and start the stack
make chaos     # full chaos run, prints the summary and exits non zero on any failed order
make down      # stop the stack
```

Unit tests run on H2 and need no Docker. The `e2e-tests` module starts Redpanda, Postgres and
Redis in containers and boots the three services in the test JVM; it runs in the `verify` phase
through failsafe (`*IT` classes).

## Conventions

* Single line conventional commits: `feat(order): ...`, `fix(payment): ...`, `test(e2e): ...`.
* Every business state change that emits an event goes through `OutboxWriter` inside the same
  transaction. Never call `KafkaTemplate` from business code.
* Every Kafka listener wraps its work in `IdempotentConsumer.once`.
* Resilience4j annotations must live on a bean that is called through its Spring proxy; do not
  call a decorated method from inside the same class.
* Keep the chaos summary honest: a change that turns a killed service into a failed order is a
  regression even if the tests pass.

## Adding a service

1. Create a module with the `common` dependency and scan `io.ledgermesh.common`.
2. Give it its own database (add it to `deploy/postgres-init.sql`) and its own consumer group.
3. Add a multi-stage Dockerfile modelled on the existing ones and a compose entry.
4. Add it to the `build` matrix in `.gitlab-ci.yml`.
