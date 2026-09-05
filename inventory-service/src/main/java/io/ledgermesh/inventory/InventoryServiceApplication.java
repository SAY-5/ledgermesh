package io.ledgermesh.inventory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication(scanBasePackages = {"io.ledgermesh.common", "io.ledgermesh.inventory"})
@EntityScan(basePackages = {"io.ledgermesh.common", "io.ledgermesh.inventory"})
@EnableJpaRepositories(basePackages = {"io.ledgermesh.common", "io.ledgermesh.inventory"})
public class InventoryServiceApplication {

  public static void main(String[] args) {
    SpringApplication.run(InventoryServiceApplication.class, args);
  }
}
