package example.orders;

import io.temporal.activity.ActivityInterface;

/** Option C: explicit business projection, written at business-meaningful points. */
@ActivityInterface
public interface ProjectionActivities {
  void recordStage(String entityId, String workflowId, String stage, String status, int stageSeq);
}
