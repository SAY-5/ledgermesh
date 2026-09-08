const REPO = "https://github.com/SAY-5/ledgermesh";

export function Footer() {
  return (
    <footer className="footer">
      <div className="wrap footer-grid">
        <div>
          <p className="eyebrow">about this page</p>
          <p className="footer-text">
            Everything above runs in your browser: a TypeScript port of the real services with the
            same transition table, outbox relay, idempotent consumer, breaker windows, time limits,
            retry policy, deferred queue and synthetic processor. The real stack is Spring Boot 3 on
            JDK 21, Redpanda (Kafka), Redis and Postgres on Docker Compose, with the chaos test run
            by GitLab CI and GitHub Actions. Time here is virtual and seeded, so a run is
            reproducible; the JVM boot after a kill is modelled as 2.5 s.
          </p>
        </div>
        <div className="footer-links">
          <a href={REPO} target="_blank" rel="noreferrer">
            github.com/SAY-5/ledgermesh
          </a>
          <a href={`${REPO}/blob/main/ARCHITECTURE.md`} target="_blank" rel="noreferrer">
            ARCHITECTURE.md
          </a>
          <a href={`${REPO}/blob/main/chaos/run.sh`} target="_blank" rel="noreferrer">
            chaos/run.sh
          </a>
          <a href={`${REPO}/tree/main/web/src/sim`} target="_blank" rel="noreferrer">
            web/src/sim (the port)
          </a>
        </div>
      </div>
    </footer>
  );
}
