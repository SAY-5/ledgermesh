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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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
    return boot(PaymentServiceApplication.class, "payments", paymentPort);
  }

  static ConfigurableApplicationContext bootOrder() {
    return boot(
        OrderServiceApplication.class,
        "orders",
        orderPort,
        "ledgermesh.inventory.url=http://localhost:" + inventoryPort);
  }

  private static ConfigurableApplicationContext boot(
      Class<?> app, String database, int port, String... extra) {
    List<String> props = new ArrayList<>();
    props.add("server.port=" + port);
    props.add("spring.jmx.enabled=false");
    props.add("spring.main.banner-mode=off");
    props.add("spring.datasource.url=" + jdbcUrl(database));
    props.add("spring.datasource.username=" + POSTGRES.getUsername());
    props.add("spring.datasource.password=" + POSTGRES.getPassword());
    props.add("spring.kafka.bootstrap-servers=" + REDPANDA.getBootstrapServers());
    props.addAll(List.of(extra));
    return new SpringApplicationBuilder(app)
        .web(WebApplicationType.SERVLET)
        .properties(props.toArray(String[]::new))
        .run();
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
    String body =
        """
        {"customerId":"%s","items":[{"sku":"%s","quantity":%d,"unitPrice":%s}]}
        """
            .formatted(customer, sku, quantity, unitPrice.toPlainString());
    JsonNode response = send("POST", "http://localhost:" + orderPort + "/orders", body);
    return response.get("id").asText();
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

  static JsonNode send(String method, String url, String body) {
    HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(url));
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
