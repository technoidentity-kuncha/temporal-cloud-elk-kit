package example.orders;

import static org.junit.jupiter.api.Assertions.*;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.stub.StreamObserver;
import io.temporal.api.workflowservice.v1.*;
import io.temporal.envconfig.ClientConfigProfile;
import io.temporal.envconfig.LoadClientConfigProfileOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class EnvironmentConfigurationTest {
  @TempDir Path temp;

  private ClientConfigProfile load(String toml, Map<String, String> env) throws Exception {
    return ClientConfigProfile.load(LoadClientConfigProfileOptions.newBuilder()
        .setConfigFileData(toml.getBytes(StandardCharsets.UTF_8))
        .setConfigFileProfile("test").setEnvOverrides(env).build());
  }

  @Test void environmentOverridesProfileAndApiKeyEnablesTls() throws Exception {
    ClientConfigProfile config = load("""
        [profile.test]
        address = "profile:7233"
        namespace = "profile-ns"
        """, Map.of("TEMPORAL_ADDRESS", "override:7233", "TEMPORAL_NAMESPACE", "override-ns",
            "TEMPORAL_API_KEY", "test-only-key"));
    assertEquals("override:7233", config.toWorkflowServiceStubsOptions().getTarget());
    assertEquals("override-ns", config.toWorkflowClientOptions().getNamespace());
    assertTrue(config.toWorkflowServiceStubsOptions().getEnableHttps());
  }

  @Test void invalidProfileFailsInsteadOfConnectingToDefault() {
    assertThrows(Exception.class, () -> load("[profile.other]\nnamespace='other'", Map.of()));
  }

  @Test void missingCertificateFailsBeforeConnecting() {
    assertThrows(Exception.class, () -> load("""
        [profile.test.tls]
        client_cert_path = "/does-not-exist/client.pem"
        client_key_path = "/does-not-exist/client.key"
        """, Map.of()).toWorkflowServiceStubsOptions());
  }

  @Test void profileCompletesMutualTlsHandshake() throws Exception {
    Path cert = temp.resolve("cert.pem");
    Path key = temp.resolve("key.pem");
    Process openssl = new ProcessBuilder("openssl", "req", "-x509", "-newkey", "rsa:2048",
        "-nodes", "-keyout", key.toString(), "-out", cert.toString(), "-days", "1",
        "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1")
        .redirectErrorStream(true).redirectOutput(temp.resolve("openssl.log").toFile()).start();
    assertTrue(openssl.waitFor(20, TimeUnit.SECONDS));
    assertEquals(0, openssl.exitValue());
    Server server = NettyServerBuilder.forPort(0)
        .sslContext(GrpcSslContexts.forServer(cert.toFile(), key.toFile())
            .trustManager(cert.toFile()).clientAuth(ClientAuth.REQUIRE).build())
        .addService(new WorkflowServiceGrpc.WorkflowServiceImplBase() {
          @Override public void getSystemInfo(GetSystemInfoRequest request,
              StreamObserver<GetSystemInfoResponse> response) {
            response.onNext(GetSystemInfoResponse.newBuilder().setServerVersion("test-mtls").build());
            response.onCompleted();
          }
        }).build().start();
    try {
      String toml = """
          [profile.test]
          address = "localhost:%d"
          namespace = "default"
          [profile.test.tls]
          client_cert_path = "%s"
          client_key_path = "%s"
          server_ca_cert_path = "%s"
          """.formatted(server.getPort(), cert, key, cert);
      WorkflowServiceStubs service = WorkflowServiceStubs.newServiceStubs(
          load(toml, Map.of()).toWorkflowServiceStubsOptions());
      try {
        assertEquals("test-mtls", service.blockingStub().withDeadlineAfter(5, TimeUnit.SECONDS)
            .getSystemInfo(GetSystemInfoRequest.getDefaultInstance()).getServerVersion());
      } finally { service.shutdownNow(); }
    } finally { server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS); }
  }
}
