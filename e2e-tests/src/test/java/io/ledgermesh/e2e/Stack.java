package io.ledgermesh.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ledgermesh.inventory.InventoryServiceApplication;
import io.ledgermesh.order.OrderServiceApplication;
import io.ledgermesh.payment.PaymentServiceApplication;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * One Redpanda, one Postgres (three databases) and one Redis, plus the three services booted in
 * this JVM on fixed free ports. Started once per test run; contexts can be restarted individually
 * to simulate a service going away and coming back.
 */
public final class Stack {

  static final RedpandaContainer REDPANDA =
      new RedpandaContainer(
          DockerImageName.parse("redpandadata/redpanda:v24.3.18")
              .asCompatibleSubstituteFor("docker.redpanda.com/redpandadata/redpanda"));
  static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");
  static final GenericContainer<?> REDIS =
      new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

  static final int DLQ_ATTEMPTS = 3;
  static final int DLQ_MAX_REPLAYS = 1;

  /**
   * The three services share one classpath in this JVM, so the Redis starter that only the
   * inventory service declares would also auto-configure a Redis client, and a Redis health check,
   * in the order and payment contexts. Their images never carry Redis; keeping it out of their
   * contexts here keeps the health they report independent of whatever answers on the host's port
   * 6379.
   */
  static final String NO_REDIS =
      "spring.autoconfigure.exclude="
          + "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,"
          + "org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration,"
          + "org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration";

  static final ObjectMapper JSON = new ObjectMapper();
  static final HttpClient HTTP = HttpClient.newHttpClient();

  static int orderPort;
  static int inventoryPort;
  static int paymentPort;
  static ConfigurableApplicationContext order;
  static ConfigurableApplicationContext inventory;
  static ConfigurableApplicationContext payment;

  private static boolean started;

  private Stack() {}

  static synchronized void start() {
    if (started) {
      return;
    }
    REDPANDA.start();
    POSTGRES.start();
    REDIS.start();
    createDatabases("orders", "inventory", "payments");
    orderPort = freePort();
    inventoryPort = freePort();
    paymentPort = freePort();
    inventory = bootInventory();
    payment = bootPayment();
    order = bootOrder();
    Runtime.getRuntime().addShutdownHook(new Thread(Stack::stop));
    started = true;
  }

  static void stop() {
    for (ConfigurableApplicationContext ctx : List.of(order, payment, inventory)) {
      if (ctx != null && ctx.isActive()) {
        ctx.close();
      }
    }
    REDIS.stop();
    POSTGRES.stop();
    REDPANDA.stop();
  }

  static ConfigurableApplicationContext bootInventory() {
    return boot(
        InventoryServiceApplication.class,
        "inventory",
        inventoryPort,
        "spring.data.redis.host=" + REDIS.getHost(),
        "spring.data.redis.port=" + REDIS.getMappedPort(6379),
        "ledgermesh.inventory.seed=");
  }

  static ConfigurableApplicationContext bootPayment() {
    return boot(PaymentServiceApplication.class, "payments", paymentPort, NO_REDIS);
  }

  static ConfigurableApplicationContext bootOrder() {
    return boot(
        OrderServiceApplication.class,
        "orders",
        orderPort,
        NO_REDIS,
        "ledgermesh.inventory.url=http://localhost:" + inventoryPort);
  }

  private static ConfigurableApplicationContext boot(
      Class<?> app, String database, int port, String... extra) {
    List<String> args = new ArrayList<>();
    args.add("--server.port=" + port);
    args.add("--spring.jmx.enabled=false");
    args.add("--spring.main.banner-mode=off");
    args.add("--spring.datasource.url=" + jdbcUrl(database));
    args.add("--spring.datasource.username=" + POSTGRES.getUsername());
    args.add("--spring.datasource.password=" + POSTGRES.getPassword());
    args.add("--spring.kafka.bootstrap-servers=" + REDPANDA.getBootstrapServers());
    // A short dead letter ladder: a record that always fails is dead lettered in under a second,
    // and one replay is enough to use up its allowance.
    args.add("--ledgermesh.dlq.max-attempts=" + DLQ_ATTEMPTS);
    args.add("--ledgermesh.dlq.initial-backoff-ms=100");
    args.add("--ledgermesh.dlq.max-backoff-ms=200");
    args.add("--ledgermesh.dlq.max-replays=" + DLQ_MAX_REPLAYS);
    for (String property : extra) {
      args.add("--" + property);
    }
    // Command line arguments outrank application.yml; builder defaults would not.
    return new SpringApplicationBuilder(app)
        .web(WebApplicationType.SERVLET)
        .run(args.toArray(String[]::new));
  }

  private static void createDatabases(String... names) {
    try (Connection connection =
            DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        Statement statement = connection.createStatement()) {
      for (String name : names) {
        statement.execute("CREATE DATABASE " + name);
      }
    } catch (SQLException e) {
      throw new IllegalStateException("could not create service databases", e);
    }
  }

  static String jdbcUrl(String database) {
    return "jdbc:postgresql://"
        + POSTGRES.getHost()
        + ":"
        + POSTGRES.getMappedPort(5432)
        + "/"
        + database;
  }

  private static int freePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static String createOrder(String customer, String sku, int quantity, BigDecimal unitPrice) {
    return send("POST", ordersUrl(), orderBody(customer, sku, quantity, unitPrice))
        .get("id")
        .asText();
  }

  static JsonNode createOrder(
      String idempotencyKey, String customer, String sku, int quantity, BigDecimal unitPrice) {
    return send(
        "POST",
        ordersUrl(),
        orderBody(customer, sku, quantity, unitPrice),
        "Idempotency-Key",
        idempotencyKey);
  }

  static String ordersUrl() {
    return "http://localhost:" + orderPort + "/orders";
  }

  static String orderBody(String customer, String sku, int quantity, BigDecimal unitPrice) {
    return """
        {"customerId":"%s","items":[{"sku":"%s","quantity":%d,"unitPrice":%s}]}
        """
        .formatted(customer, sku, quantity, unitPrice.toPlainString());
  }

  static JsonNode getOrder(String id) {
    return send("GET", "http://localhost:" + orderPort + "/orders/" + id, null);
  }

  static String orderStatus(String id) {
    return getOrder(id).get("status").asText();
  }

  static void putStock(String sku, int available) {
    send(
        "PUT",
        "http://localhost:" + inventoryPort + "/stock/" + sku,
        "{\"available\":" + available + "}");
  }

  static int stock(String sku) {
    return send("GET", "http://localhost:" + inventoryPort + "/stock/" + sku, null)
        .get("available")
        .asInt();
  }

  static JsonNode stockViaOrderService(String sku) {
    return send("GET", "http://localhost:" + orderPort + "/stock/" + sku, null);
  }

  static JsonNode send(String method, String url, String body, String... headers) {
    HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(url));
    if (headers.length > 0) {
      request.headers(headers);
    }
    if (body == null) {
      request.method(method, HttpRequest.BodyPublishers.noBody());
    } else {
      request
          .header("Content-Type", "application/json")
          .method(method, HttpRequest.BodyPublishers.ofString(body));
    }
    try {
      HttpResponse<String> response =
          HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() >= 300) {
        throw new IllegalStateException(
            method + " " + url + " -> " + response.statusCode() + " " + response.body());
      }
      return JSON.readTree(response.body());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
