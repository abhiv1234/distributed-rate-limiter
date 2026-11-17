package dev.ratelimiter.server;

import dev.ratelimiter.api.RateLimitRule;
import dev.ratelimiter.config.LimiterConfig;
import dev.ratelimiter.config.RateLimiterRegistry;
import dev.ratelimiter.redis.RedisConfig;
import dev.ratelimiter.resilience.FailureMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** End-to-end over HTTP, in LOCAL mode so it needs no Redis. */
class RateLimiterServerIT {

    private RateLimiterServer server;
    private HttpClient http;
    private String base;

    @BeforeEach
    void setUp() throws Exception {
        LimiterConfig config = new LimiterConfig(0, LimiterConfig.Mode.LOCAL,
                RedisConfig.defaults("localhost", 6379), FailureMode.FAIL_OPEN, 1, 5, 1000,
                List.of(RateLimitRule.tokenBucket("api", 3, 0.001),
                        RateLimitRule.tokenBucket("demo", 2, 0.001)));
        server = new RateLimiterServer(new RateLimiterRegistry(config), 0);
        server.start();
        http = HttpClient.newHttpClient();
        base = "http://localhost:" + server.port();
    }

    @AfterEach
    void tearDown() {
        server.close();
    }

    private HttpResponse<String> post(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create(base + path))
                .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(String path, String clientId) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + path)).GET();
        if (clientId != null) {
            b.header("X-Client-Id", clientId);
        }
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void returns200ThenRateLimited429WithHeaders() throws Exception {
        for (int i = 0; i < 3; i++) {
            HttpResponse<String> r = post("/v1/ratelimit/api?key=user-1");
            assertEquals(200, r.statusCode());
            assertEquals(String.valueOf(2 - i), r.headers().firstValue("RateLimit-Remaining").orElseThrow());
        }
        HttpResponse<String> limited = post("/v1/ratelimit/api?key=user-1");
        assertEquals(429, limited.statusCode());
        assertTrue(limited.body().contains("\"allowed\":false"), limited.body());
        assertTrue(Long.parseLong(limited.headers().firstValue("Retry-After").orElseThrow()) >= 1);
        assertEquals("3", limited.headers().firstValue("RateLimit-Limit").orElseThrow());

        assertEquals(200, post("/v1/ratelimit/api?key=user-2").statusCode(), "other keys unaffected");
    }

    @Test
    void validatesInput() throws Exception {
        assertEquals(404, post("/v1/ratelimit/nope?key=a").statusCode());
        assertEquals(400, post("/v1/ratelimit/api").statusCode());
        assertEquals(400, post("/v1/ratelimit/api?key=a&permits=x").statusCode());
        assertEquals(400, post("/v1/ratelimit/api?key=a&permits=99").statusCode());
    }

    @Test
    void demoEndpointIsProtectedPerClient() throws Exception {
        assertEquals(200, get("/demo/hello", "alice").statusCode());
        assertEquals(200, get("/demo/hello", "alice").statusCode());
        assertEquals(429, get("/demo/hello", "alice").statusCode());
        assertEquals(200, get("/demo/hello", "bob").statusCode());
    }

    @Test
    void exposesHealthRulesAndMetrics() throws Exception {
        assertTrue(get("/healthz", null).body().contains("\"status\":\"UP\""));
        assertTrue(get("/v1/rules", null).body().contains("\"name\":\"api\""));
        post("/v1/ratelimit/api?key=m");
        String metrics = get("/metrics", null).body();
        assertTrue(metrics.contains("ratelimiter_decisions_total{rule=\"api\",outcome=\"allowed\"} 1"), metrics);
        assertTrue(metrics.contains("ratelimiter_decision_latency_seconds_count{rule=\"api\"} 1"), metrics);
    }
}
