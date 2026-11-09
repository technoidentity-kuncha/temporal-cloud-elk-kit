package example.orders;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import example.orders.audit.ArchiveOnCloseInterceptor;
import io.temporal.client.WorkflowFailedException;
import io.temporal.client.WorkflowOptions;
import io.temporal.common.WorkflowExecutionHistory;
import io.temporal.failure.ApplicationFailure;
import io.temporal.testing.TestEnvironmentOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactoryOptions;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Option E: archive-on-close interceptor. */
class ArchiveOnCloseInterceptorTest {

  static final String QUEUE = "orders";
  static final String ARCHIVER_QUEUE = "history-archiver";

  /** Stand-in for the Python HistoryArchiverWorkflow: records which runs asked to be archived. */
  @WorkflowInterface
  public interface FakeArchiver {
    @WorkflowMethod(name = ArchiveOnCloseInterceptor.ARCHIVER_TYPE)
    int run(String workflowId, String runId);
  }

  static final ConcurrentLinkedQueue<String> archived = new ConcurrentLinkedQueue<>();

  public static class FakeArchiverImpl implements FakeArchiver {
    @Override
    public int run(String workflowId, String runId) {
      archived.add(workflowId + "|" + runId);
      return 0;
    }
  }

  /** Deterministic activities: everything succeeds, unless the payment is set to be declined. */
  static class Activities implements OrderActivities {
    final boolean decline;

    Activities(boolean decline) {
      this.decline = decline;
    }

    @Override public void validateOrder(String id) {}
    @Override public void reserveInventory(String id) {}
    @Override public String scheduleShipment(String id) { return "ship-" + id; }
    @Override public void notifyCustomer(String id, String message) {}

    @Override
    public String chargePayment(String id) {
      if (decline) {
        throw ApplicationFailure.newNonRetryableFailure("card declined", "PaymentDeclined");
      }
      return "txn-" + id;
    }
  }

  private TestWorkflowEnvironment env;

  private TestWorkflowEnvironment start(boolean withInterceptor, boolean decline) {
    return start(withInterceptor, decline, false);
  }

  private TestWorkflowEnvironment start(boolean withInterceptor, boolean decline, boolean noCache) {
    archived.clear();
    WorkerFactoryOptions.Builder f = WorkerFactoryOptions.newBuilder();
    if (noCache) {
      f.setWorkflowCacheSize(0); // every workflow task replays the full history from event 1
    }
    if (withInterceptor) {
      f.setWorkerInterceptors(new ArchiveOnCloseInterceptor(ARCHIVER_QUEUE));
    }
    env = TestWorkflowEnvironment.newInstance(TestEnvironmentOptions.newBuilder().setWorkerFactoryOptions(f.build()).build());
    Worker w = env.newWorker(QUEUE);
    w.registerWorkflowImplementationTypes(OrderWorkflowImpl.class, ShipmentWorkflowImpl.class);
    w.registerActivitiesImplementations(new Activities(decline));
    env.newWorker(ARCHIVER_QUEUE).registerWorkflowImplementationTypes(FakeArchiverImpl.class);
    env.start();
    return env;
  }

  private OrderWorkflow stub(String id) {
    return env.getWorkflowClient()
        .newWorkflowStub(OrderWorkflow.class, WorkflowOptions.newBuilder().setTaskQueue(QUEUE).setWorkflowId(id).build());
  }

  private Set<String> archivedWorkflowIds() {
    env.sleep(Duration.ofSeconds(5)); // archivers are abandoned children; give them a moment
    return archived.stream().map(s -> s.split("\\|")[0]).collect(Collectors.toSet());
  }

  @AfterEach
  void tearDown() {
    if (env != null) env.close();
  }

  @Test
  void completedWorkflowAndItsChildAreArchived() {
    start(true, false);
    assertEquals("txn-o-1/ship-o-1", stub("order-1").process("o-1"));
    assertEquals(Set.of("order-1", "order-1/shipment"), archivedWorkflowIds());
  }

  @Test
  void failedWorkflowIsArchivedAndFailureIsUnchanged() {
    start(true, true);
    WorkflowFailedException e = assertThrows(WorkflowFailedException.class, () -> stub("order-2").process("o-2"));
    assertTrue(e.getCause() instanceof ApplicationFailure);
    assertEquals("OrderFailed", ((ApplicationFailure) e.getCause()).getType());
    assertEquals(Set.of("order-2"), archivedWorkflowIds());
  }

  @Test
  void historiesRecordedBeforeRolloutStillReplay() throws Exception {
    // 1. Record a history on a worker WITHOUT the interceptor (a workflow in flight before rollout).
    start(false, false);
    stub("order-3").process("o-3");
    WorkflowExecutionHistory oldHistory = env.getWorkflowClient().fetchHistory("order-3");
    assertTrue(archived.isEmpty());
    env.close();

    // 2. Replay it on a worker WITH the interceptor: must not raise a non-determinism error.
    replayWithInterceptor(oldHistory);
  }

  /** Replays a history on a fresh worker that has the interceptor registered. */
  private static void replayWithInterceptor(WorkflowExecutionHistory history) throws Exception {
    try (TestWorkflowEnvironment replayEnv =
        TestWorkflowEnvironment.newInstance(
            TestEnvironmentOptions.newBuilder()
                .setWorkerFactoryOptions(
                    WorkerFactoryOptions.newBuilder()
                        .setWorkerInterceptors(new ArchiveOnCloseInterceptor(ARCHIVER_QUEUE))
                        .build())
                .build())) {
      Worker replayer = replayEnv.newWorker("replay-only");
      replayer.registerWorkflowImplementationTypes(OrderWorkflowImpl.class, ShipmentWorkflowImpl.class);
      replayer.replayWorkflowExecution(history);
    }
  }

  @Test
  void interceptedWorkflowsReplayDeterministically() {
    // Cache disabled: each workflow task re-runs the workflow from the start against the recorded
    // history (real run IDs), so any non-determinism introduced by the interceptor would fail here.
    start(true, false, true);
    assertEquals("txn-o-4/ship-o-4", stub("order-4").process("o-4"));
    assertEquals(Set.of("order-4", "order-4/shipment"), archivedWorkflowIds());
  }

}
