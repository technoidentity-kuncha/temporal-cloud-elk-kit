package example.orders.audit;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.temporal.activity.ActivityExecutionContext;
import io.temporal.activity.ActivityInfo;
import io.temporal.common.interceptors.ActivityInboundCallsInterceptor;
import io.temporal.common.interceptors.ActivityInboundCallsInterceptorBase;
import io.temporal.common.interceptors.WorkerInterceptorBase;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Set;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Option B: records every activity attempt (STARTED, then COMPLETED or FAILED) to Postgres without
 * any code in the workflows. Registered once per worker, so new workflows are covered
 * automatically.
 *
 * <p>Runs on the activity side, where I/O is allowed. Workflow-side interceptors must not do I/O
 * (determinism), so workflow start/close, signals and timers come from the history loader
 * (options A/D) instead.
 *
 * <p>An audit write failure is counted and logged, never thrown: the business activity's result is
 * unaffected.
 */
public class AuditWorkerInterceptor extends WorkerInterceptorBase {

  private static final Logger log = LoggerFactory.getLogger(AuditWorkerInterceptor.class);

  private static final String INSERT =
      "INSERT INTO audit_step_event (event_key, workflow_id, run_id, workflow_type, task_queue, activity_type, "
          + "activity_id, attempt, phase, occurred_at, duration_ms, error_type, error_message, worker_sdk) "
          + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'java') ON CONFLICT (event_key) DO NOTHING";

  private final DataSource dataSource;
  private final Set<String> excludedActivityTypes;
  private final Counter writeFailures;
  private final Counter writes;

  public AuditWorkerInterceptor(
      DataSource dataSource, Set<String> excludedActivityTypes, MeterRegistry registry) {
    this.dataSource = dataSource;
    this.excludedActivityTypes = excludedActivityTypes;
    this.writes = Counter.builder("audit_writes").tag("sdk", "java").register(registry);
    this.writeFailures = Counter.builder("audit_write_failures").tag("sdk", "java").register(registry);
  }

  @Override
  public ActivityInboundCallsInterceptor interceptActivity(ActivityInboundCallsInterceptor next) {
    return new ActivityInboundCallsInterceptorBase(next) {
      private ActivityExecutionContext context;

      @Override
      public void init(ActivityExecutionContext context) {
        this.context = context;
        super.init(context);
      }

      @Override
      public ActivityOutput execute(ActivityInput input) {
        ActivityInfo info = context.getInfo();
        if (excludedActivityTypes.contains(info.getActivityType())) {
          return super.execute(input);
        }
        Instant start = Instant.now();
        record(info, "STARTED", start, null, null);
        try {
          ActivityOutput out = super.execute(input);
          record(info, "COMPLETED", Instant.now(), start, null);
          return out;
        } catch (RuntimeException e) {
          record(info, "FAILED", Instant.now(), start, e);
          throw e;
        }
      }
    };
  }

  private void record(ActivityInfo info, String phase, Instant at, Instant startedAt, Throwable error) {
    String key = info.getRunId() + ":" + info.getActivityId() + ":" + info.getAttempt() + ":" + phase;
    try (Connection c = dataSource.getConnection();
        PreparedStatement ps = c.prepareStatement(INSERT)) {
      ps.setString(1, key);
      ps.setString(2, info.getWorkflowId());
      ps.setString(3, info.getRunId());
      ps.setString(4, info.getWorkflowType());
      ps.setString(5, info.getActivityTaskQueue());
      ps.setString(6, info.getActivityType());
      ps.setString(7, info.getActivityId());
      ps.setInt(8, info.getAttempt());
      ps.setString(9, phase);
      ps.setTimestamp(10, Timestamp.from(at));
      if (startedAt != null) {
        ps.setLong(11, at.toEpochMilli() - startedAt.toEpochMilli());
      } else {
        ps.setNull(11, java.sql.Types.BIGINT);
      }
      ps.setString(12, error == null ? null : error.getClass().getSimpleName());
      ps.setString(13, error == null ? null : truncate(error.getMessage()));
      ps.executeUpdate();
      writes.increment();
    } catch (Exception e) {
      writeFailures.increment();
      log.warn("audit write failed ({}): {}", key, e.getMessage());
    }
  }

  private static String truncate(String s) {
    return s == null || s.length() <= 1000 ? s : s.substring(0, 1000);
  }
}
