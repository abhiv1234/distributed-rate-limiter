package dev.ratelimiter.server;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dev.ratelimiter.api.Decision;
import dev.ratelimiter.api.RateLimitRule;
import dev.ratelimiter.api.RateLimiter;
import dev.ratelimiter.config.LimiterConfig;
import dev.ratelimiter.config.RateLimiterRegistry;
import dev.ratelimiter.metrics.Metrics;
import dev.ratelimiter.resilience.FailoverRateLimiter;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Rate limiting as a service, on the JDK's built-in HTTP server with one virtual thread per request
 * (Java 21): blocking Redis I/O is cheap, so the code stays simple and synchronous.
 *
 * <pre>
 * POST /v1/ratelimit/{rule}?key=K&amp;permits=N   200 allowed | 429 denied (+ RateLimit-* headers)
 * GET  /v1/rules                               configured rules
 * GET  /demo/hello                             sample endpoint protected by rule "demo" (X-Client-Id)
 * GET  /healthz                                liveness + Redis status
 * GET  /metrics                                Prometheus metrics
 * </pre>
 */
public final class RateLimiterServer implements AutoCloseable {

    private final RateLimiterRegistry registry;
    private final Metrics metrics = new Metrics();
    private final HttpServer http;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public RateLimiterServer(RateLimiterRegistry registry, int port) throws IOException {
        this.registry = registry;
        this.http = HttpServer.create(new InetSocketAddress(port), 1024);
        http.setExecutor(executor);
        http.createContext("/v1/ratelimit/", ex -> safely(ex, this::handleRateLimit));
        http.createContext("/v1/rules", ex -> safely(ex, this::handleRules));
        http.createContext("/demo/hello", ex -> safely(ex, this::handleDemo));
        http.createContext("/healthz", ex -> safely(ex, this::handleHealth));
        http.createContext("/metrics", ex -> safely(ex, this::handleMetrics));
    }

    public static void main(String[] args) throws IOException {
        LimiterConfig config = LimiterConfig.load();
        RateLimiterRegistry registry = new RateLimiterRegistry(config);
        RateLimiterServer server = new RateLimiterServer(registry, config.serverPort());
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(server::close, "shutdown"));
        System.out.printf("rate limiter listening on :%d mode=%s failureMode=%s rules=%s%n",
                server.port(), config.mode(), config.failureMode(),
                config.rules().stream().map(RateLimitRule::name).toList());
    }

    public void start() {
        http.start();
    }

    public int port() {
        return http.getAddress().getPort();
    }

    public Metrics metrics() {
        return metrics;
    }

    @Override
    public void close() {
        http.stop(1);
        executor.shutdown();
        registry.close();
    }

    // ---- handlers ------------------------------------------------------------------------------

    private void handleRateLimit(HttpExchange ex) throws IOException {
        if (!ex.getRequestMethod().equals("POST") && !ex.getRequestMethod().equals("GET")) {
            send(ex, 405, json("error", "use POST"));
            return;
        }
        String ruleName = ex.getRequestURI().getPath().substring("/v1/ratelimit/".length());
        Optional<RateLimiter> limiter = registry.get(ruleName);
        if (limiter.isEmpty()) {
            send(ex, 404, json("error", "unknown rule: " + ruleName));
            return;
        }
        Map<String, String> q = query(ex.getRequestURI().getRawQuery());
        String key = q.get("key");
        int permits;
        try {
            permits = Integer.parseInt(q.getOrDefault("permits", "1"));
        } catch (NumberFormatException e) {
            send(ex, 400, json("error", "permits must be an integer"));
            return;
        }
        Decision d;
        try {
            d = decide(limiter.get(), key, permits);
        } catch (IllegalArgumentException e) {
            send(ex, 400, json("error", e.getMessage()));
            return;
        }
        writeRateLimitHeaders(ex.getResponseHeaders(), limiter.get().rule(), d);
        String body = "{\"allowed\":" + d.allowed()
                + ",\"rule\":\"" + esc(ruleName) + "\""
                + ",\"key\":\"" + esc(key) + "\""
                + ",\"limit\":" + d.limit()
                + ",\"remaining\":" + d.remaining()
                + ",\"retryAfterMs\":" + d.retryAfterMillis()
                + ",\"resetAfterMs\":" + d.resetAfterMillis()
                + ",\"degraded\":" + d.degraded() + "}";
        send(ex, d.allowed() ? 200 : 429, body);
    }

    /** Example of using the limiter as middleware in front of a real endpoint. */
    private void handleDemo(HttpExchange ex) throws IOException {
        RateLimiter limiter = registry.get("demo").orElse(null);
        if (limiter == null) {
            send(ex, 404, json("error", "rule 'demo' not configured"));
            return;
        }
        String client = Optional.ofNullable(ex.getRequestHeaders().getFirst("X-Client-Id"))
                .filter(s -> !s.isBlank())
                .orElse(ex.getRemoteAddress().getAddress().getHostAddress());
        Decision d = decide(limiter, client, 1);
        writeRateLimitHeaders(ex.getResponseHeaders(), limiter.rule(), d);
        if (!d.allowed()) {
            send(ex, 429, json("error", "rate limit exceeded, retry in " + d.retryAfterMillis() + " ms"));
            return;
        }
        send(ex, 200, json("message", "hello, " + client));
    }

    private void handleRules(HttpExchange ex) throws IOException {
        StringBuilder sb = new StringBuilder("[");
        for (RateLimiter l : registry.all()) {
            RateLimitRule r = l.rule();
            if (sb.length() > 1) {
                sb.append(',');
            }
            sb.append("{\"name\":\"").append(esc(r.name())).append("\",\"algorithm\":\"").append(r.algorithm())
                    .append("\",\"limit\":").append(r.limit());
            switch (r.algorithm()) {
                case TOKEN_BUCKET -> sb.append(",\"refillPerSecond\":").append(r.refillPerSecond());
                default -> sb.append(",\"windowMs\":").append(r.windowMillis());
            }
            sb.append('}');
        }
        send(ex, 200, sb.append(']').toString());
    }

    private void handleHealth(HttpExchange ex) throws IOException {
        String redis = registry.pool() == null ? "N/A" : (registry.pool().ping() ? "UP" : "DOWN");
        // Liveness stays 200 when Redis is down: the failure mode keeps the service useful.
        send(ex, 200, "{\"status\":\"UP\",\"mode\":\"" + registry.config().mode() + "\",\"redis\":\"" + redis + "\"}");
    }

    private void handleMetrics(HttpExchange ex) throws IOException {
        StringBuilder out = new StringBuilder(4096);
        metrics.render(out);
        out.append("# TYPE ratelimiter_circuit_open gauge\n");
        for (RateLimiter l : registry.all()) {
            if (l instanceof FailoverRateLimiter f) {
                Metrics.line(out, "ratelimiter_circuit_open", "rule=\"" + l.rule().name() + "\"",
                        f.circuitState() == dev.ratelimiter.resilience.CircuitBreaker.State.CLOSED ? 0 : 1);
            }
        }
        out.append("# TYPE ratelimiter_local_tracked_keys gauge\n");
        Metrics.line(out, "ratelimiter_local_tracked_keys", "", registry.trackedLocalKeys());
        if (registry.pool() != null) {
            out.append("# TYPE ratelimiter_redis_open_connections gauge\n");
            Metrics.line(out, "ratelimiter_redis_open_connections", "", registry.pool().openConnections());
        }
        ex.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4");
        byte[] b = out.toString().getBytes(StandardCharsets.UTF_8);
        ex.sendResponseHeaders(200, b.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(b);
        }
    }

    // ---- helpers -------------------------------------------------------------------------------

    private Decision decide(RateLimiter limiter, String key, int permits) {
        long t0 = System.nanoTime();
        Decision d = limiter.tryAcquire(key, permits);
        metrics.record(limiter.rule().name(), d, (System.nanoTime() - t0) / 1000);
        return d;
    }

    /**
     * Headers per the IETF "RateLimit header fields for HTTP" draft, plus the standard Retry-After.
     * Values in seconds, rounded up.
     */
    static void writeRateLimitHeaders(Headers h, RateLimitRule rule, Decision d) {
        h.set("RateLimit-Limit", Long.toString(d.limit()));
        h.set("RateLimit-Remaining", Long.toString(d.remaining()));
        h.set("RateLimit-Reset", Long.toString(ceilSeconds(d.resetAfterMillis())));
        String policy = switch (rule.algorithm()) {
            case TOKEN_BUCKET -> d.limit() + ";burst=" + d.limit() + ";rate=" + rule.refillPerSecond();
            default -> d.limit() + ";w=" + ceilSeconds(rule.windowMillis());
        };
        h.set("RateLimit-Policy", policy);
        if (!d.allowed()) {
            h.set("Retry-After", Long.toString(Math.max(1, ceilSeconds(d.retryAfterMillis()))));
        }
        if (d.degraded()) {
            h.set("X-RateLimit-Degraded", "true");
        }
    }

    private static long ceilSeconds(long millis) {
        return (millis + 999) / 1000;
    }

    private interface Handler {
        void handle(HttpExchange ex) throws IOException;
    }

    private static void safely(HttpExchange ex, Handler h) {
        try (ex) {
            try {
                h.handle(ex);
            } catch (RuntimeException e) {
                send(ex, 500, json("error", e.getClass().getSimpleName() + ": " + e.getMessage()));
            }
        } catch (IOException ignored) {
            // client went away
        }
    }

    private static void send(HttpExchange ex, int status, String body) throws IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, b.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(b);
        }
    }

    static Map<String, String> query(String raw) {
        Map<String, String> m = new HashMap<>();
        if (raw == null || raw.isEmpty()) {
            return m;
        }
        for (String pair : raw.split("&")) {
            int eq = pair.indexOf('=');
            String k = eq < 0 ? pair : pair.substring(0, eq);
            String v = eq < 0 ? "" : pair.substring(eq + 1);
            m.put(URLDecoder.decode(k, StandardCharsets.UTF_8), URLDecoder.decode(v, StandardCharsets.UTF_8));
        }
        return m;
    }

    private static String json(String field, String value) {
        return "{\"" + field + "\":\"" + esc(value) + "\"}";
    }

    static String esc(String s) {
        if (s == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder(s.length() + 8);
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.toString();
    }
}
