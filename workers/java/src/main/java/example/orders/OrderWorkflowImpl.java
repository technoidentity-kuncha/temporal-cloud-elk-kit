package example.orders;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ActivityFailure;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.Workflow;
import java.time.Duration;

public class OrderWorkflowImpl implements OrderWorkflow {

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

  @Override
  public String process(String orderId) {
    String workflowId = Workflow.getInfo().getWorkflowId();
    try {
      activities.validateOrder(orderId);
      activities.reserveInventory(orderId);
      String txn = activities.chargePayment(orderId);

      ShipmentWorkflow shipment =
          Workflow.newChildWorkflowStub(
              ShipmentWorkflow.class,
              ChildWorkflowOptions.newBuilder().setWorkflowId(workflowId + "/shipment").build());
      String shipmentId = shipment.ship(orderId);

      activities.notifyCustomer(orderId, "order shipped: " + shipmentId);
      return txn + "/" + shipmentId;
    } catch (ActivityFailure e) {
      throw ApplicationFailure.newFailureWithCause("order failed", "OrderFailed", e);
    }
  }
}
