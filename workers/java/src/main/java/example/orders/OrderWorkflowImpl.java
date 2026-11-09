package example.orders;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ActivityFailure;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import org.slf4j.Logger;

public class OrderWorkflowImpl implements OrderWorkflow {

  private static final Logger log = Workflow.getLogger(OrderWorkflowImpl.class);

  private final OrderActivities activities =
      Workflow.newActivityStub(
          OrderActivities.class,
          ActivityOptions.newBuilder()
              .setStartToCloseTimeout(Duration.ofSeconds(10))
              .setRetryOptions(
                  RetryOptions.newBuilder()
                      .setInitialInterval(Duration.ofMillis(500))
                      .setMaximumAttempts(5)
                      .setDoNotRetry("PaymentDeclined")
                      .build())
              .build());

  // Projection writes: bounded retries for the demo. In production use a long ScheduleToClose
  // (hours) so a database outage delays the projection instead of dropping it.
  private final ProjectionActivities projection =
      Workflow.newActivityStub(
          ProjectionActivities.class,
          ActivityOptions.newBuilder()
              .setStartToCloseTimeout(Duration.ofSeconds(5))
              .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(3).build())
              .build());

  private int stageSeq = 0;

  @Override
  public String process(String orderId) {
    String workflowId = Workflow.getInfo().getWorkflowId();
    try {
      activities.validateOrder(orderId);
      project(orderId, workflowId, "VALIDATED", "IN_PROGRESS");

      activities.reserveInventory(orderId);
      project(orderId, workflowId, "RESERVED", "IN_PROGRESS");

      String txn = activities.chargePayment(orderId);
      project(orderId, workflowId, "CHARGED", "IN_PROGRESS");

      ShipmentWorkflow shipment =
          Workflow.newChildWorkflowStub(
              ShipmentWorkflow.class,
              ChildWorkflowOptions.newBuilder().setWorkflowId(workflowId + "/shipment").build());
      String shipmentId = shipment.ship(orderId);
      project(orderId, workflowId, "SHIPPED", "IN_PROGRESS");

      activities.notifyCustomer(orderId, "order shipped: " + shipmentId);
      project(orderId, workflowId, "DONE", "COMPLETED");
      return txn + "/" + shipmentId;
    } catch (ActivityFailure e) {
      project(orderId, workflowId, "FAILED", "FAILED");
      throw ApplicationFailure.newFailureWithCause("order failed", "OrderFailed", e);
    }
  }

  /**
   * Non-fatal projection write. If the projection store stays down past the retry budget, the
   * business workflow carries on; the gap is visible in metrics/logs and can be repaired from
   * history (options A/D).
   */
  private void project(String orderId, String workflowId, String stage, String status) {
    stageSeq++;
    try {
      projection.recordStage(orderId, workflowId, stage, status, stageSeq);
    } catch (ActivityFailure e) {
      log.warn("projection write skipped for {} stage {}: {}", orderId, stage, e.getMessage());
    }
  }
}
