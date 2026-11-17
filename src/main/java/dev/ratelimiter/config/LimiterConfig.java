package dev.ratelimiter.config;

import dev.ratelimiter.api.Algorithm;
import dev.ratelimiter.api.RateLimitRule;
import dev.ratelimiter.redis.RedisConfig;
import dev.ratelimiter.resilience.FailureMode;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;

/**
 * Service configuration from a properties file plus environment overrides.
 *
 * <p>Any property can be overridden by an environment variable: upper-case it, replace '.' with
 * '_' and prefix {@code RL_} (e.g. {@code rule.api.limit} becomes {@code RL_RULE_API_LIMIT}).
 * {@code REDIS_HOST}, {@code REDIS_PORT} and {@code REDIS_PASSWORD} are accepted as shortcuts.
 */
public record LimiterConfig(int serverPort, Mode mode, RedisConfig redis, FailureMode failureMode,
                            int expectedNodes, int breakerFailureThreshold, long breakerOpenMillis,
                            List<RateLimitRule> rules) {

    public enum Mode { LOCAL, REDIS }

    public static LimiterConfig load() {
        Properties p = new Properties();
        String path = System.getenv("RL_CONFIG");
        try (InputStream in = path != null
                ? Files.newInputStream(Path.of(path))
                : LimiterConfig.class.getResourceAsStream("/ratelimiter.properties")) {
            if (in != null) {
                p.load(in);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read config " + path, e);
        }
        applyEnv(p, System.getenv());
        return from(p);
    }

    static void applyEnv(Properties p, Map<String, String> env) {
        alias(p, env, "REDIS_HOST", "redis.host");
        alias(p, env, "REDIS_PORT", "redis.port");
        alias(p, env, "REDIS_PASSWORD", "redis.password");
        env.forEach((k, v) -> {
            if (k.startsWith("RL_") && !k.equals("RL_CONFIG")) {
                p.setProperty(k.substring(3).toLowerCase(Locale.ROOT).replace('_', '.'), v);
            }
        });
    }

    private static void alias(Properties p, Map<String, String> env, String var, String prop) {
        if (env.get(var) != null) {
            p.setProperty(prop, env.get(var));
        }
    }

    public static LimiterConfig from(Properties p) {
        RedisConfig redis = new RedisConfig(
                p.getProperty("redis.host", "localhost"),
                intProp(p, "redis.port", 6379),
                p.getProperty("redis.password"),
                intProp(p, "redis.pool.size", 64),
                intProp(p, "redis.connect.timeout.ms", 200),
                intProp(p, "redis.read.timeout.ms", 100),
                intProp(p, "redis.borrow.timeout.ms", 50));
        return new LimiterConfig(
                intProp(p, "server.port", 8080),
                Mode.valueOf(p.getProperty("mode", "LOCAL").trim().toUpperCase(Locale.ROOT)),
                redis,
                FailureMode.valueOf(p.getProperty("failure.mode", "LOCAL_FALLBACK").trim().toUpperCase(Locale.ROOT)),
                intProp(p, "failure.expected.nodes", 1),
                intProp(p, "breaker.failure.threshold", 5),
                intProp(p, "breaker.open.ms", 2000),
                parseRules(p));
    }

    static List<RateLimitRule> parseRules(Properties p) {
        TreeSet<String> names = new TreeSet<>();
        for (String k : p.stringPropertyNames()) {
            if (k.startsWith("rule.")) {
                String rest = k.substring(5);
                int dot = rest.indexOf('.');
                if (dot > 0) {
                    names.add(rest.substring(0, dot));
                }
            }
        }
        List<RateLimitRule> rules = new ArrayList<>();
        for (String n : names) {
            String base = "rule." + n + ".";
            Algorithm alg = Algorithm.valueOf(required(p, base + "algorithm").trim().toUpperCase(Locale.ROOT));
            long limit = Long.parseLong(required(p, base + "limit").trim());
            long window = Long.parseLong(p.getProperty(base + "window.ms", "0").trim());
            double refill = Double.parseDouble(p.getProperty(base + "refill.per.second", "0").trim());
            rules.add(new RateLimitRule(n, alg, limit, window, refill));
        }
        return List.copyOf(rules);
    }

    private static String required(Properties p, String key) {
        String v = p.getProperty(key);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("missing required property: " + key);
        }
        return v;
    }

    private static int intProp(Properties p, String key, int def) {
        String v = p.getProperty(key);
        return v == null || v.isBlank() ? def : Integer.parseInt(v.trim());
    }
}
