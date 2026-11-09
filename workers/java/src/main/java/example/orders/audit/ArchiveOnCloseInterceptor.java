package example.orders.audit;

import io.temporal.api.enums.v1.ParentClosePolicy;
import io.temporal.common.interceptors.WorkerInterceptorBase;
import io.temporal.common.interceptors.WorkflowInboundCallsInterceptor;
import io.temporal.common.interceptors.WorkflowInboundCallsInterceptorBase;
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptor;
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptorBase;
import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.ChildWorkflowStub;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInfo;
import org.slf4j.Logger;

/**
 * Option E: when any workflow's code finishes (completed, failed, cancelled or continue-as-new),
 * start the history archiver as an ABANDONed child. The archiver waits for this run to close, then
 * fetches its history (close event included) for the Postgres loader.
 *
 * <p>Registered once on the WorkerFactory. The parent waits for child-start confirmation.
 * Caught start errors are logged and swallowed, and the original result or
 * exception is returned unchanged. Terminated / timed-out runs never reach this code; a periodic
 * poll sweep (history_loader.py poll) covers them.
 *
 * <p>Rollout safety: starting a child is a new workflow command, so the call is gated with
 * Workflow.getVersion. Runs that started before this interceptor existed replay without it.
 */
public class ArchiveOnCloseInterceptor extends WorkerInterceptorBase {

  public static final String ARCHIVER_TYPE = "HistoryArchiverWorkflow";
  public static final String CHANGE_ID = "archive-on-close";

  private final String archiverTaskQueue;

  public ArchiveOnCloseInterceptor(String archiverTaskQueue) {
    this.archiverTaskQueue = archiverTaskQueue;
  }

  @Override
  public WorkflowInboundCallsInterceptor interceptWorkflow(WorkflowInboundCallsInterceptor next) {
    return new WorkflowInboundCallsInterceptorBase(next) {
      @Override
      public void init(WorkflowOutboundCallsInterceptor outbound) {
        // Continue-as-new unwinds the workflow thread with an Error, so hook the call itself.
        super.init(
            new WorkflowOutboundCallsInterceptorBase(outbound) {
              @Override
              public void continueAsNew(ContinueAsNewInput input) {
                startArchiver();
                super.continueAsNew(input);
              }
            });
      }

      @Override
      public WorkflowOutput execute(WorkflowInput input) {
        WorkflowOutput out;
        try {
          out = super.execute(input);
        } catch (RuntimeException failure) {
          // Failed or cancelled: archive, then surface the original failure unchanged.
          // Errors (e.g. the SDK unwinding an evicted workflow thread) are deliberately not caught.
          startArchiver();
          throw failure;
        }
        startArchiver();
        return out;
      }
    };
  }

  private void startArchiver() {
    WorkflowInfo info = Workflow.getInfo();
    if (ARCHIVER_TYPE.equals(info.getWorkflowType())) {
      return;
    }
    // Detached scope: still runs when the workflow itself was cancelled.
    Workflow.newDetachedCancellationScope(
            () -> {
              if (Workflow.getVersion(CHANGE_ID, Workflow.DEFAULT_VERSION, 1) == Workflow.DEFAULT_VERSION) {
                return; // run started before the interceptor was deployed
              }
              Logger log = Workflow.getLogger(ArchiveOnCloseInterceptor.class);
              try {
                ChildWorkflowStub archiver =
                    Workflow.newUntypedChildWorkflowStub(
                        ARCHIVER_TYPE,
                        ChildWorkflowOptions.newBuilder()
                            .setWorkflowId("archive/" + info.getRunId())
                            .setTaskQueue(archiverTaskQueue)
                            .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON)
                            .build());
                archiver.executeAsync(Integer.class, info.getWorkflowId(), info.getRunId());
                archiver.getExecution().get(); // wait until started, not until finished
              } catch (RuntimeException e) {
                log.warn("history archiver not started for {}: {}", info.getWorkflowId(), e.toString());
              }
            })
        .run();
  }
}
