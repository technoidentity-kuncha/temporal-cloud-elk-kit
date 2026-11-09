package example.orders;

import com.sun.net.httpserver.HttpServer;
import com.uber.m3.tally.RootScopeBuilder;
import com.uber.m3.tally.Scope;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import example.orders.audit.ArchiveOnCloseInterceptor;
import example.orders.audit.AuditWorkerInterceptor;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.common.reporter.MicrometerClientStatsReporter;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.worker.WorkerFactoryOptions;
import io.temporal.worker.WorkerOptions;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Set;

/**
 * Worker entrypoint. Connects to Temporal Cloud with an API key, exposes SDK metrics in Prometheus
 * text format on :METRICS_PORT/metrics (scraped by Metricbeat → Elasticsearch), and registers the
 * audit interceptor.
 */
public class WorkerMain {

  static String env(String name, String def) {
    String v = System.getenv(name);
    return v == null || v.isBlank() ? def : v;
  }

  public static void main(String[] args) throws Exception {
    String address = env("TEMPORAL_ADDRESS", "localhost:7233");
    String namespace = env("TEMPORAL_NAMESPACE", "default");
    String apiKey = env("TEMPORAL_API_KEY", "");
    String taskQueue = env("TASK_QUEUE", "orders-java");
    int metricsPort = Integer.parseInt(env("METRICS_PORT", "9464"));

    // 1. Metrics: Temporal's Tally scope → Micrometer → Prometheus text exposition.
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    registry.config().commonTags("sdk", "java", "service", env("SERVICE_NAME", "orders-worker-java"));
    Scope metricsScope =
        new RootScopeBuilder()
            .reporter(new MicrometerClientStatsReporter(registry))
            .reportEvery(com.uber.m3.util.Duration.ofSeconds(10));
    startMetricsServer(registry, metricsPort);

    // 2. Connection to Temporal Cloud (API key) or a local dev server (no key).
    WorkflowServiceStubsOptions.Builder stubs =
        WorkflowServiceStubsOptions.newBuilder().setTarget(address).setMetricsScope(metricsScope);
    if (!apiKey.isBlank()) {
      stubs.setEnableHttps(true).addApiKey(() -> apiKey);
    }
    WorkflowServiceStubs service = WorkflowServiceStubs.newServiceStubs(stubs.build());
    WorkflowClient client =
        WorkflowClient.newInstance(
            service, WorkflowClientOptions.newBuilder().setNamespace(namespace).build());

    // 3. Audit store.
    HikariConfig pg = new HikariConfig();
    pg.setJdbcUrl(env("PG_URL", "jdbc:postgresql://localhost:55432/audit"));
    pg.setUsername(env("PG_USER", "temporal"));
    pg.setPassword(env("PG_PASSWORD", "temporal"));
    pg.setMaximumPoolSize(10);
    pg.setConnectionTimeout(2000);
    pg.setInitializationFailTimeout(-1); // start even if Postgres is down: audit is non-fatal
    HikariDataSource dataSource = new HikariDataSource(pg);

    // 4. Worker with the audit interceptor. Projection writes are excluded from the step audit.
    WorkerFactory factory =
        WorkerFactory.newInstance(
            client,
            WorkerFactoryOptions.newBuilder()
                .setWorkerInterceptors(
                    new AuditWorkerInterceptor(dataSource, Set.of("RecordStage"), registry),
                    new ArchiveOnCloseInterceptor(env("ARCHIVER_TASK_QUEUE", "history-archiver")))
                .build());
    Worker worker =
        factory.newWorker(
            taskQueue,
            WorkerOptions.newBuilder()
                // Deliberately small so slot exhaustion is visible on the dashboard under load.
                .setMaxConcurrentActivityExecutionSize(
                    Integer.parseInt(env("MAX_ACTIVITY_SLOTS", "20")))
                .build());
    worker.registerWorkflowImplementationTypes(OrderWorkflowImpl.class, ShipmentWorkflowImpl.class);
    worker.registerActivitiesImplementations(
        new OrderActivitiesImpl(), new ProjectionActivitiesImpl(dataSource));

    factory.start();
    System.out.printf(
        "Java worker started: namespace=%s taskQueue=%s metrics=:%d/metrics%n",
        namespace, taskQueue, metricsPort);
  }

  private static void startMetricsServer(PrometheusMeterRegistry registry, int port)
      throws java.io.IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
    server.createContext(
        "/metrics",
        exchange -> {
          byte[] body = registry.scrape().getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
          }
        });
    server.start();
  }
}
