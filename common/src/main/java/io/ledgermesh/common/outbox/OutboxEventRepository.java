package io.ledgermesh.common.outbox;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

  List<OutboxEvent> findTop200ByPublishedAtIsNullOrderByIdAsc();

  long countByPublishedAtIsNull();
}
