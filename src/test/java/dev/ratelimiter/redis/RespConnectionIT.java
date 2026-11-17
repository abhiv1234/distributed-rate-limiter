package dev.ratelimiter.redis;

import dev.ratelimiter.testing.RedisTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RespConnectionIT {

    private RedisPool pool;
    private String key;

    @BeforeEach
    void setUp() {
        pool = RedisTestSupport.poolOrSkip();
        key = RedisTestSupport.uniquePrefix() + ":resp";
    }

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.execute(c -> c.call("DEL", key));
            pool.close();
        }
    }

    @Test
    void decodesAllReplyTypes() {
        pool.execute(c -> {
            assertEquals("PONG", c.call("PING"));                       // simple string
            assertEquals("OK", c.call("SET", key, "héllo wörld"));
            assertEquals("héllo wörld", c.call("GET", key));             // bulk string, UTF-8
            assertEquals(1L, c.call("DEL", key));                        // integer
            assertNull(c.call("GET", key));                              // null bulk
            assertEquals(List.of(1L, "two", List.of(3L)),
                    c.call("EVAL", "return {1, 'two', {3}}", "0"));      // nested array
            return null;
        });
    }

    @Test
    void errorRepliesThrowButKeepConnectionUsable() {
        pool.execute(c -> {
            c.call("SET", key, "not-a-hash");
            RedisReplyException e = assertThrows(RedisReplyException.class, () -> c.call("HGET", key, "f"));
            assertTrue(e.getMessage().startsWith("WRONGTYPE"));
            assertFalse(c.isBroken());
            assertEquals("PONG", c.call("PING"));
            return null;
        });
    }

    @Test
    void unreachableRedisFailsFast() {
        RedisPool dead = new RedisPool(new RedisConfig("127.0.0.1", 1, null, 2, 100, 100, 100));
        long t0 = System.nanoTime();
        assertThrows(RedisConnectionException.class, () -> dead.execute(c -> c.call("PING")));
        assertTrue((System.nanoTime() - t0) / 1_000_000 < 1000, "should fail within the connect timeout");
        assertFalse(dead.ping());
        dead.close();
    }
}
