package gcm.md.sequencer.cluster.metrics;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

/**
 * A minimal {@code /metrics} Prometheus-scrape endpoint for {@code cluster-node} (design §17).
 * Uses the JDK's built-in {@link HttpServer} rather than pulling in a web framework — this module
 * deliberately has no Spring dependency (design §3.3), so anything heavier would be a boundary
 * violation for the sake of one endpoint.
 */
public final class MetricsHttpServer implements AutoCloseable {

    private final HttpServer server;

    public MetricsHttpServer(PrometheusMeterRegistry registry, int port) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/metrics", exchange -> {
            byte[] body = registry.scrape().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "text/plain; version=0.0.4");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream responseBody = exchange.getResponseBody()) {
                responseBody.write(body);
            }
        });
        server.setExecutor(Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, "cluster-node-metrics-http");
            thread.setDaemon(true);
            return thread;
        }));
        server.start();
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
