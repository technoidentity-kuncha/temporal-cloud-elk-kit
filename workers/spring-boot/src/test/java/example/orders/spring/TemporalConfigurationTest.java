package example.orders.spring;

import static org.junit.jupiter.api.Assertions.*;

import com.uber.m3.tally.NoopScope;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.client.schedules.ScheduleClientOptions;
import io.temporal.envconfig.ClientConfigProfile;
import io.temporal.envconfig.LoadClientConfigProfileOptions;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TemporalConfigurationTest {
  @Test void noProfilePreservesMetricsAndUsesDefaultNamespace() throws Exception {
    ClientConfigProfile profile = ClientConfigProfile.load(LoadClientConfigProfileOptions.newBuilder()
        .setDisableFile(true).setEnvOverrides(Map.of()).build());
    TemporalConfiguration configuration = new TemporalConfiguration();
    NoopScope scope = new NoopScope();
    WorkflowServiceStubsOptions options = configuration.connectionOptions(profile)
        .customize(WorkflowServiceStubsOptions.newBuilder().setMetricsScope(scope)).build();
    assertSame(scope, options.getMetricsScope());
    assertEquals("default", configuration.clientOptions(profile)
        .customize(WorkflowClientOptions.newBuilder()).build().getNamespace());
    assertEquals("default", configuration.scheduleOptions(profile)
        .customize(ScheduleClientOptions.newBuilder()).build().getNamespace());
  }
}
