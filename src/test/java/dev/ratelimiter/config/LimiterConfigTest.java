package dev.ratelimiter.config;

import dev.ratelimiter.api.Algorithm;
import dev.ratelimiter.api.RateLimitRule;
import dev.ratelimiter.resilience.FailureMode;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LimiterConfigTest {

    private static Properties bundled() throws Exception {
        Properties p = new Properties();
        try (InputStream in = LimiterConfig.class.getResourceAsStream("/ratelimiter.properties")) {
            p.load(in);
        }
        return p;
    }

    @Test
    void parsesBundledConfig() throws Exception {
        LimiterConfig c = LimiterConfig.from(bundled());
        assertEquals(LimiterConfig.Mode.LOCAL, c.mode());
        assertEquals(FailureMode.LOCAL_FALLBACK, c.failureMode());
        assertEquals(4, c.rules().size());
        RateLimitRule login = c.rules().stream().filter(r -> r.name().equals("login")).findFirst().orElseThrow();
        assertEquals(Algorithm.SLIDING_WINDOW_LOG, login.algorithm());
        assertEquals(5, login.limit());
        assertEquals(60_000, login.windowMillis());
    }

    @Test
    void environmentOverridesProperties() throws Exception {
        Properties p = bundled();
        LimiterConfig.applyEnv(p, Map.of(
                "RL_MODE", "redis",
                "REDIS_HOST", "redis.internal",
                "RL_RULE_API_LIMIT", "250",
                "RL_RULE_NEW_ALGORITHM", "SLIDING_WINDOW_COUNTER",
                "RL_RULE_NEW_LIMIT", "7",
                "RL_RULE_NEW_WINDOW_MS", "1000"));
        LimiterConfig c = LimiterConfig.from(p);
        assertEquals(LimiterConfig.Mode.REDIS, c.mode());
        assertEquals("redis.internal", c.redis().host());
        assertEquals(250, c.rules().stream().filter(r -> r.name().equals("api")).findFirst().orElseThrow().limit());
        assertEquals(5, c.rules().size(), "a whole new rule can be defined from env");
    }

    @Test
    void rejectsIncompleteRules() {
        Properties p = new Properties();
        p.setProperty("rule.x.algorithm", "TOKEN_BUCKET");
        p.setProperty("rule.x.limit", "10");
        // missing refill.per.second
        assertThrows(IllegalArgumentException.class, () -> LimiterConfig.from(p));
    }
}
