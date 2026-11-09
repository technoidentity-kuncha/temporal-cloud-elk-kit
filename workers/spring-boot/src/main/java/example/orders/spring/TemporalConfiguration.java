package example.orders.spring;

import example.orders.OrderActivitiesImpl;
import example.orders.audit.ArchiveOnCloseInterceptor;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.client.schedules.ScheduleClientOptions;
import io.temporal.envconfig.ClientConfigProfile;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.spring.boot.TemporalOptionsCustomizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class TemporalConfiguration {
  @Bean
  ClientConfigProfile temporalProfile() throws java.io.IOException {
    return ClientConfigProfile.load();
  }

  @Bean
  TemporalOptionsCustomizer<WorkflowServiceStubsOptions.Builder> connectionOptions(
      ClientConfigProfile profile) {
    // Resolve auth through the SDK; retain the starter's Micrometer bridge and plugins.
    return builder -> {
      WorkflowServiceStubsOptions original = builder.build();
      WorkflowServiceStubsOptions.Builder resolved =
          WorkflowServiceStubsOptions.newBuilder(profile.toWorkflowServiceStubsOptions())
              .setMetricsScope(original.getMetricsScope());
      if (original.getPlugins() != null) resolved.setPlugins(original.getPlugins());
      return resolved;
    };
  }

  @Bean
  TemporalOptionsCustomizer<WorkflowClientOptions.Builder> clientOptions(ClientConfigProfile profile) {
    return builder -> builder.setNamespace(namespace(profile));
  }

  @Bean
  TemporalOptionsCustomizer<ScheduleClientOptions.Builder> scheduleOptions(ClientConfigProfile profile) {
    return builder -> builder.setNamespace(namespace(profile));
  }

  private String namespace(ClientConfigProfile profile) {
    return WorkflowClientOptions.newBuilder(profile.toWorkflowClientOptions())
        .validateAndBuildWithDefaults().getNamespace();
  }

  @Bean
  OrderActivitiesImpl orderActivities() {
    return new OrderActivitiesImpl();
  }

  @Bean
  ArchiveOnCloseInterceptor archiveOnCloseInterceptor(
      @Value("${ARCHIVER_TASK_QUEUE:history-archiver}") String taskQueue) {
    return new ArchiveOnCloseInterceptor(taskQueue);
  }
}
