import example.orders.audit.ArchiveOnCloseInterceptor;
import io.temporal.client.WorkflowClient;
import io.temporal.failure.ApplicationFailure;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.worker.WorkerFactoryOptions;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/** Business fixture only. The production Java interceptor calls the real Python archiver. */
public class ArchivalTestWorker {
  @WorkflowInterface
  public interface Lifecycle {
    @WorkflowMethod(name = "JavaLifecycle") String run(String mode);
    @SignalMethod void release();
    @QueryMethod boolean ready();
  }

  public static class LifecycleImpl implements Lifecycle {
    private boolean released;

    @Override
    public String run(String mode) {
      if (mode.equals("wait")) Workflow.await(() -> released);
      if (mode.equals("fail")) {
        throw ApplicationFailure.newNonRetryableFailure("expected failure", "BusinessFailure");
      }
      if (mode.equals("continue")) Workflow.continueAsNew("complete");
      return "done";
    }

    @Override public void release() { released = true; }
    @Override public boolean ready() { return true; }
  }

  public static void main(String[] args) throws Exception {
    WorkflowServiceStubs service = WorkflowServiceStubs.newServiceStubs(
        WorkflowServiceStubsOptions.newBuilder().setTarget(args[0]).build());
    WorkerFactory factory = WorkerFactory.newInstance(WorkflowClient.newInstance(service),
        WorkerFactoryOptions.newBuilder()
            .setWorkflowCacheSize(0) // Replay on every workflow task.
            .setWorkerInterceptors(new ArchiveOnCloseInterceptor("history-archiver")).build());
    Worker worker = factory.newWorker(args[1]);
    worker.registerWorkflowImplementationTypes(LifecycleImpl.class);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      factory.shutdownNow();
      service.shutdownNow();
    }));
    factory.start();
    Thread.currentThread().join();
  }
}
