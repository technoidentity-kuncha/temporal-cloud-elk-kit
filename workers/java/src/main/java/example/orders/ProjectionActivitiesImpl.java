package example.orders;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.sql.DataSource;

public class ProjectionActivitiesImpl implements ProjectionActivities {

  // stage_seq guard: an older stage arriving late (retry, reordering) never overwrites a newer one.
  private static final String UPSERT =
      "INSERT INTO business_projection (entity_id, workflow_id, stage, status, stage_seq, updated_at) "
          + "VALUES (?, ?, ?, ?, ?, now()) "
          + "ON CONFLICT (entity_id) DO UPDATE SET workflow_id = EXCLUDED.workflow_id, stage = EXCLUDED.stage, "
          + "status = EXCLUDED.status, stage_seq = EXCLUDED.stage_seq, updated_at = now() "
          + "WHERE business_projection.stage_seq <= EXCLUDED.stage_seq";

  private final DataSource dataSource;

  public ProjectionActivitiesImpl(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public void recordStage(String entityId, String workflowId, String stage, String status, int stageSeq) {
    try (Connection c = dataSource.getConnection();
        PreparedStatement ps = c.prepareStatement(UPSERT)) {
      ps.setString(1, entityId);
      ps.setString(2, workflowId);
      ps.setString(3, stage);
      ps.setString(4, status);
      ps.setInt(5, stageSeq);
      ps.executeUpdate();
    } catch (SQLException e) {
      // Let Temporal retry it; the workflow decides what happens when retries are exhausted.
      throw new RuntimeException("projection write failed: " + e.getMessage(), e);
    }
  }
}
