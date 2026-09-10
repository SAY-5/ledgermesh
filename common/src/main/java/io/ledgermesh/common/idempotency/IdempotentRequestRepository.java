package io.ledgermesh.common.idempotency;

import org.springframework.data.jpa.repository.JpaRepository;

public interface IdempotentRequestRepository extends JpaRepository<IdempotentRequest, String> {}
