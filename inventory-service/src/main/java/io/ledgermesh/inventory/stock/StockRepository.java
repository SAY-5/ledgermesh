package io.ledgermesh.inventory.stock;

import jakarta.persistence.LockModeType;
import java.util.Collection;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface StockRepository extends JpaRepository<StockItem, String> {

  /** Row locks taken in sku order so concurrent multi line orders cannot deadlock. */
  @Lock(LockModeType.PESSIMISTIC_WRITE)
  @Query("select s from StockItem s where s.sku in :skus order by s.sku")
  List<StockItem> lockAllBySku(@Param("skus") Collection<String> skus);
}
