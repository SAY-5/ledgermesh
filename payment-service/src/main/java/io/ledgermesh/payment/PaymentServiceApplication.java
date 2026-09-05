package io.ledgermesh.payment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication(scanBasePackages = {"io.ledgermesh.common", "io.ledgermesh.payment"})
@EntityScan(basePackages = {"io.ledgermesh.common", "io.ledgermesh.payment"})
@EnableJpaRepositories(basePackages = {"io.ledgermesh.common", "io.ledgermesh.payment"})
public class PaymentServiceApplication {

  public static void main(String[] args) {
    SpringApplication.run(PaymentServiceApplication.class, args);
  }
}
