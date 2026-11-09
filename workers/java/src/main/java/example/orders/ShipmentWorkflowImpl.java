package example.orders;

import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.Workflow;
import java.time.Duration;

public class ShipmentWorkflowImpl implements ShipmentWorkflow {

  private final OrderActivities activities =
      Workflow.newActivityStub(
          OrderActivities.class,
          ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(10)).build());

  @Override
  public String ship(String orderId) {
    String id = activities.scheduleShipment(orderId);
    Workflow.sleep(Duration.ofSeconds(2)); // a durable timer, so timers show up in history too
    return id;
  }
}
