package example.orders;

import com.sun.net.httpserver.HttpServer;
import com.uber.m3.tally.RootScopeBuilder;
import com.uber.m3.tally.Scope;
import example.orders.audit.ArchiveOnCloseInterceptor;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.temporal.client.WorkflowClient;
import io.temporal.common.reporter.MicrometerClientStatsReporter;
import io.temporal.envconfig.ClientConfigProfile;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.worker.WorkerFactoryOptions;
import io.temporal.worker.WorkerOptions;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * Connects using Temporal Environment Configuration (API key or mTLS), exposes Prometheus-format
 * SDK metrics on :METRICS_PORT/metrics, and registers the archive-on-close interceptor.
 */
public class WorkerMain {

  static String env(String name, String def) {
    String v = System.getenv(name);
    return v == null || v.isBlank() ? def : v;
  }

  public static void main(String[] args) throws Exception {
    String taskQueue = env("TASK_QUEUE", "orders-java");
    int metricsPort = Integer.parseInt(env("METRICS_PORT", "9464"));

    // Validate profiles and certificate files before opening the metrics server.
    ClientConfigProfile profile = ClientConfigProfile.load();
    WorkflowServiceStubsOptions connection = profile.toWorkflowServiceStubsOptions();

    // 1. Metrics: Temporal's Tally scope → Micrometer → Prometheus text exposition.
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    registry.config().commonTags("sdk", "java", "service", env("SERVICE_NAME", "orders-worker-java"));
    Scope metricsScope =
        new RootScopeBuilder()
            .reporter(new MicrometerClientStatsReporter(registry))
            .reportEvery(com.uber.m3.util.Duration.ofSeconds(10));
    startMetricsServer(registry, metricsPort);

    // 2. Standard profiles + environment overrides; TLS is resolved by the SDK.
    WorkflowServiceStubs service = WorkflowServiceStubs.newServiceStubs(
        WorkflowServiceStubsOptions.newBuilder(connection).setMetricsScope(metricsScope).build());
    WorkflowClient client = WorkflowClient.newInstance(service, profile.toWorkflowClientOptions());

    // 3. Worker. The close interceptor starts an independent history archiver.
    WorkerFactory factory =
        WorkerFactory.newInstance(
            client,
            WorkerFactoryOptions.newBuilder()
                .setWorkerInterceptors(
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
    worker.registerActivitiesImplementations(new OrderActivitiesImpl());

    factory.start();
    System.out.printf(
        "Java worker started: namespace=%s taskQueue=%s metrics=:%d/metrics%n",
        client.getOptions().getNamespace(), taskQueue, metricsPort);
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
