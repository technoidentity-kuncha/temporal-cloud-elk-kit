package example.orders;

import static org.junit.jupiter.api.Assertions.assertEquals;


import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The audit/projection store being down must never fail the business workflow. */
class OrderWorkflowTest {

  private TestWorkflowEnvironment env;
  private Worker worker;
  private WorkflowClient client;

  /** Deterministic business activities: everything succeeds. */
  public static class HappyActivities implements OrderActivities {
    @Override public void validateOrder(String id) {}
    @Override public void reserveInventory(String id) {}
    @Override public String chargePayment(String id) { return "txn-" + id; }
    @Override public String scheduleShipment(String id) { return "ship-" + id; }
    @Override public void notifyCustomer(String id, String message) {}
  }

  /** Projection store that is always down. */
  static class BrokenProjection implements ProjectionActivities {
    final AtomicInteger attempts = new AtomicInteger();

    @Override
    public void recordStage(String entityId, String workflowId, String stage, String status, int seq) {
      attempts.incrementAndGet();
      throw new RuntimeException("connection refused");
    }
  }

  @BeforeEach
  void setUp() {
    env = TestWorkflowEnvironment.newInstance();
    worker = env.newWorker("test-queue");
    worker.registerWorkflowImplementationTypes(OrderWorkflowImpl.class, ShipmentWorkflowImpl.class);
    client = env.getWorkflowClient();
  }

  @AfterEach
  void tearDown() {
    env.close();
  }

  @Test
  void workflowCompletesWhenProjectionStoreIsDown() {
    BrokenProjection projection = new BrokenProjection();
    worker.registerActivitiesImplementations(new HappyActivities(), projection);
    env.start();

    OrderWorkflow wf =
        client.newWorkflowStub(
            OrderWorkflow.class,
            WorkflowOptions.newBuilder().setTaskQueue("test-queue").setWorkflowId("order-test-1").build());
    String result = wf.process("o-1");

    assertEquals("txn-o-1/ship-o-1", result);
    // 5 stages (VALIDATED, RESERVED, CHARGED, SHIPPED, DONE) x 3 attempts: every write retried, then skipped.
    assertEquals(15, projection.attempts.get());
  }

  @Test
  void projectionIsWrittenWhenStoreIsUp() {
    AtomicInteger writes = new AtomicInteger();
    ProjectionActivities ok = (e, w, stage, status, seq) -> writes.incrementAndGet();
    worker.registerActivitiesImplementations(new HappyActivities(), ok);
    env.start();

    OrderWorkflow wf =
        client.newWorkflowStub(
            OrderWorkflow.class,
            WorkflowOptions.newBuilder().setTaskQueue("test-queue").setWorkflowId("order-test-2").build());
    wf.process("o-2");
    assertEquals(5, writes.get());
  }
}
