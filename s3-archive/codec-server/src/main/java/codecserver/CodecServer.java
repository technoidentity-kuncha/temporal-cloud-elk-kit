package codecserver;

import io.temporal.rde.httpserver.RDEHttpServer;
import java.util.List;

/** Official Java Remote Data Encoder (sdk-java temporal-remote-data-encoder) wrapping CryptCodec. */
public class CodecServer {
  public static void main(String[] args) throws Exception {
    int port = Integer.parseInt(System.getenv().getOrDefault("CODEC_PORT", "8888"));
    RDEHttpServer server = new RDEHttpServer(List.of(new CryptCodec()), port);
    server.start();
    System.out.println("codec server listening on http://localhost:" + server.getPort() + " (/encode, /decode)");
    Thread.currentThread().join();
  }
}
