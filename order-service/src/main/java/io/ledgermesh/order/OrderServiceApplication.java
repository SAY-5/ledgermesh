package io.ledgermesh.order;

import io.ledgermesh.order.saga.SagaTimeouts;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication(scanBasePackages = {"io.ledgermesh.common", "io.ledgermesh.order"})
@EntityScan(basePackages = {"io.ledgermesh.common", "io.ledgermesh.order"})
@EnableJpaRepositories(basePackages = {"io.ledgermesh.common", "io.ledgermesh.order"})
@EnableConfigurationProperties(SagaTimeouts.class)
public class OrderServiceApplication {

  public static void main(String[] args) {
    SpringApplication.run(OrderServiceApplication.class, args);
  }
}
