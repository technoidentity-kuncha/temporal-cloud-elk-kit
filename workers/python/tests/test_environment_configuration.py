"""Profile precedence and a real TLS handshake against a local gRPC fixture."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import subprocess
import tempfile
import unittest

import grpc
from temporalio.api.workflowservice.v1 import GetSystemInfoRequest, GetSystemInfoResponse
from temporalio.client import Client
from temporalio.envconfig import ClientConfig


class EnvironmentConfigurationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.addCleanup(self.directory.cleanup)

    def config(self, text, env=None):
        path = self.root / "temporal.toml"
        path.write_text(text)
        return ClientConfig.load_client_connect_config(
            profile="test", config_file=str(path), config_file_strict=True,
            override_env_vars=env or {},
        )

    def test_environment_overrides_profile_and_api_key_enables_tls(self):
        config = self.config('[profile.test]\naddress="profile:7233"\nnamespace="profile-ns"', {
            "TEMPORAL_ADDRESS": "override:7233", "TEMPORAL_NAMESPACE": "override-ns",
            "TEMPORAL_API_KEY": "test-only-key",
        })
        self.assertEqual(config["target_host"], "override:7233")
        self.assertEqual(config["namespace"], "override-ns")
        self.assertEqual(config["api_key"], "test-only-key")
        self.assertTrue(config["tls"])

    def test_missing_profile_fails_instead_of_using_default(self):
        with self.assertRaises(Exception):
            self.config('[profile.other]\nnamespace="other"')

    def test_missing_certificate_fails_before_connecting(self):
        with self.assertRaises(Exception):
            self.config('''[profile.test.tls]
client_cert_path="/does-not-exist/client.pem"
client_key_path="/does-not-exist/client.key"
''')

    async def test_profile_completes_mutual_tls_handshake(self):
        cert, key = self.root / "cert.pem", self.root / "key.pem"
        # Own the configuration: host defaults can mark the test leaf as a CA.
        openssl_config = self.root / "openssl.cnf"
        openssl_config.write_text("""[req]
distinguished_name=dn
x509_extensions=leaf
[dn]
[leaf]
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=DNS:localhost,IP:127.0.0.1
""")
        subprocess.run([
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
            "-config", str(openssl_config),
            "-keyout", str(key), "-out", str(cert), "-subj", "/CN=localhost",
        ], check=True, capture_output=True, timeout=20)
        with ThreadPoolExecutor(max_workers=2) as pool:
            server = grpc.server(pool)
            server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(
                "temporal.api.workflowservice.v1.WorkflowService", {
                    "GetSystemInfo": grpc.unary_unary_rpc_method_handler(
                        lambda request, context: GetSystemInfoResponse(server_version="test-mtls"),
                        request_deserializer=GetSystemInfoRequest.FromString,
                        response_serializer=GetSystemInfoResponse.SerializeToString,
                    )
                }),))
            port = server.add_secure_port("127.0.0.1:0", grpc.ssl_server_credentials(
                [(key.read_bytes(), cert.read_bytes())],
                root_certificates=cert.read_bytes(), require_client_auth=True,
            ))
            server.start()
            try:
                config = self.config(f'''[profile.test]
address="localhost:{port}"
namespace="default"
[profile.test.tls]
client_cert_path="{cert}"
client_key_path="{key}"
server_ca_cert_path="{cert}"
''')
                client = await Client.connect(**config)
                response = await client.workflow_service.get_system_info(GetSystemInfoRequest())
                self.assertEqual(response.server_version, "test-mtls")
            finally:
                server.stop(0).wait()


if __name__ == "__main__":
    unittest.main()
