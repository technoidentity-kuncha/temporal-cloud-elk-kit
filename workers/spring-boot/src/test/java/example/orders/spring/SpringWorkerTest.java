package example.orders.spring;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;
import static org.awaitility.Awaitility.await;

import example.orders.OrderActivitiesImpl;
import example.orders.OrderWorkflow;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.envconfig.ClientConfigProfile;
import io.temporal.envconfig.LoadClientConfigProfileOptions;
import io.temporal.testserver.TestServer;
import io.temporal.worker.WorkerFactory;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.test.context.bean.override.convention.TestBean;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@AutoConfigureObservability
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = "spring.temporal.start-workers=false")
class SpringWorkerTest {
  static final int PORT = freePort();
  static final TestServer.PortBoundTestServer SERVER = TestServer.createPortBoundServer(PORT);
  static final Set<String> ARCHIVED = ConcurrentHashMap.newKeySet();

  @Autowired WorkflowClient client;
  @Autowired WorkerFactory factory;
  @Autowired TestRestTemplate http;
  @MockitoBean(name = "orderActivities") OrderActivitiesImpl activities;
  @TestBean(methodName = "testProfile") ClientConfigProfile temporalProfile;

  static int freePort() {
    try (ServerSocket socket = new ServerSocket(0)) { return socket.getLocalPort(); }
    catch (Exception e) { throw new RuntimeException(e); }
  }

  static ClientConfigProfile testProfile() throws Exception {
    return ClientConfigProfile.load(LoadClientConfigProfileOptions.newBuilder()
        .setDisableFile(true).setEnvOverrides(Map.of("TEMPORAL_ADDRESS", "localhost:" + PORT,
            "TEMPORAL_NAMESPACE", "default")).build());
  }

  @WorkflowInterface
  public interface Archive {
    @WorkflowMethod(name = "HistoryArchiverWorkflow")
    int run(String workflowId, String runId);
  }
  public static class ArchiveImpl implements Archive {
    public int run(String workflowId, String runId) { ARCHIVED.add(workflowId); return 1; }
  }

  @Test void executesSharedWorkflowsArchivesBothRunsAndExportsMetrics() {
    when(activities.chargePayment("123")).thenReturn("txn-123");
    when(activities.scheduleShipment("123")).thenReturn("ship-123");
    factory.newWorker("history-archiver").registerWorkflowImplementationTypes(ArchiveImpl.class);
    factory.start();
    try {
      OrderWorkflow order = client.newWorkflowStub(OrderWorkflow.class, WorkflowOptions.newBuilder()
          .setWorkflowId("spring-order").setTaskQueue("orders-java")
          .setWorkflowExecutionTimeout(Duration.ofSeconds(20)).build());
      assertEquals("txn-123/ship-123", order.process("123"));
      await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
          assertEquals(Set.of("spring-order", "spring-order/shipment"), ARCHIVED));
      await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
        String body = http.getForObject("/actuator/prometheus", String.class);
        assertNotNull(body);
        assertTrue(body.contains("temporal_workflow_completed"));
        assertTrue(body.contains("sdk=\"java\""));
        assertTrue(body.contains("service=\"orders-worker-spring\""));
      });
    } finally { factory.shutdownNow(); }
  }

  @AfterAll static void closeServer() { SERVER.close(); }
}
