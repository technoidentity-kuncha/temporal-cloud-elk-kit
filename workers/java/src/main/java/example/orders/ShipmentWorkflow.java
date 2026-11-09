package example.orders;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/** Child workflow, so the dashboards and history loader see parent/child relationships. */
@WorkflowInterface
public interface ShipmentWorkflow {
  @WorkflowMethod
  String ship(String orderId);
}
