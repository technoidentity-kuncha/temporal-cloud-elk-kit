package example.orders;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/** Parent workflow: one order moving through validate → reserve → charge → ship (child) → notify. */
@WorkflowInterface
public interface OrderWorkflow {
  @WorkflowMethod
  String process(String orderId);
}
