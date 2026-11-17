package dev.ratelimiter.redis;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * A Lua script executed with EVALSHA, falling back to EVAL on NOSCRIPT (after a Redis restart,
 * failover, or SCRIPT FLUSH). The SHA-1 is computed locally, so the common path sends only the
 * 40-byte digest instead of the whole script.
 *
 * <p>Why Lua: Redis runs a script atomically (no other command interleaves), which turns the
 * read-modify-write of the limiter state into a single round trip with no race and no WATCH/MULTI
 * retry loop.
 */
public final class LuaScript {

    private final String name;
    private final String source;
    private final String sha1;

    public LuaScript(String name, String source) {
        this.name = name;
        this.source = source;
        this.sha1 = sha1Hex(source);
    }

    public static LuaScript fromClasspath(String name) {
        String path = "/lua/" + name + ".lua";
        try (InputStream in = LuaScript.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("missing script on classpath: " + path);
            }
            return new LuaScript(name, new String(in.readAllBytes(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public String name() {
        return name;
    }

    public String sha1() {
        return sha1;
    }

    /** Runs the script with one key; returns Redis' decoded reply. */
    public Object eval(RespConnection conn, String key, String... args) {
        String[] cmd = new String[3 + 1 + args.length];
        cmd[1] = sha1;
        cmd[2] = "1";
        cmd[3] = key;
        System.arraycopy(args, 0, cmd, 4, args.length);
        cmd[0] = "EVALSHA";
        try {
            return conn.call(cmd);
        } catch (RedisReplyException e) {
            if (!e.isNoScript()) {
                throw e;
            }
            cmd[0] = "EVAL";
            cmd[1] = source; // EVAL also caches the script, so later EVALSHA calls succeed
            return conn.call(cmd);
        }
    }

    private static String sha1Hex(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            return HexFormat.of().formatHex(md.digest(s.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
