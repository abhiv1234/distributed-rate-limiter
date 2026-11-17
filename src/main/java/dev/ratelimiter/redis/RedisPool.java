package dev.ratelimiter.redis;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Bounded connection pool. A semaphore caps concurrent connections at {@code poolSize}; idle
 * connections are reused from a lock-free queue and created lazily. Broken connections are
 * closed and dropped, so the pool heals itself after a Redis restart.
 */
public final class RedisPool implements AutoCloseable {

    private final RedisConfig config;
    private final Semaphore permits;
    private final ConcurrentLinkedQueue<RespConnection> idle = new ConcurrentLinkedQueue<>();
    private final AtomicInteger open = new AtomicInteger();
    private volatile boolean closed;

    public RedisPool(RedisConfig config) {
        this.config = config;
        this.permits = new Semaphore(config.poolSize(), true);
    }

    public RedisConfig config() {
        return config;
    }

    public <T> T execute(Function<RespConnection, T> work) {
        if (closed) {
            throw new RedisConnectionException("pool closed");
        }
        boolean acquired;
        try {
            acquired = permits.tryAcquire(config.borrowTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RedisConnectionException("interrupted waiting for a redis connection", e);
        }
        if (!acquired) {
            throw new RedisConnectionException("timed out waiting for a redis connection (pool exhausted)");
        }
        RespConnection conn = null;
        try {
            conn = idle.poll();
            if (conn == null) {
                conn = newConnection();
            }
            return work.apply(conn);
        } finally {
            if (conn != null) {
                if (conn.isBroken() || closed) {
                    conn.close();
                    open.decrementAndGet();
                } else {
                    idle.offer(conn);
                }
            }
            permits.release();
        }
    }

    private RespConnection newConnection() {
        RespConnection c = new RespConnection(config.host(), config.port(),
                config.connectTimeoutMs(), config.readTimeoutMs());
        open.incrementAndGet();
        if (config.password() != null && !config.password().isEmpty()) {
            try {
                c.call("AUTH", config.password());
            } catch (RuntimeException e) {
                c.close();
                open.decrementAndGet();
                throw e;
            }
        }
        return c;
    }

    public boolean ping() {
        try {
            return "PONG".equals(execute(c -> c.call("PING")));
        } catch (RuntimeException e) {
            return false;
        }
    }

    public int openConnections() {
        return open.get();
    }

    @Override
    public void close() {
        closed = true;
        RespConnection c;
        while ((c = idle.poll()) != null) {
            c.close();
            open.decrementAndGet();
        }
    }
}
