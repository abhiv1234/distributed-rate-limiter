package dev.ratelimiter.redis;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * A minimal, blocking Redis connection speaking RESP2.
 *
 * <p>Deliberately small: the limiter only needs EVAL/EVALSHA/SCRIPT/PING/AUTH. Replies decode to
 * {@code String} (simple and bulk strings), {@code Long} (integers), {@code List<Object>} (arrays) or
 * {@code null}. Error replies throw {@link RedisReplyException}; I/O failures throw
 * {@link RedisConnectionException} and mark the connection broken so the pool discards it.
 *
 * <p>Not thread-safe: a connection is owned by one borrower at a time via {@link RedisPool}.
 */
public final class RespConnection implements Closeable {

    private static final byte[] CRLF = {'\r', '\n'};

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    private boolean broken;

    public RespConnection(String host, int port, int connectTimeoutMs, int readTimeoutMs) {
        try {
            socket = new Socket();
            socket.setTcpNoDelay(true);   // tiny request/response messages: don't wait for Nagle
            socket.setKeepAlive(true);
            socket.connect(new InetSocketAddress(host, port), connectTimeoutMs);
            socket.setSoTimeout(readTimeoutMs);
            in = new BufferedInputStream(socket.getInputStream(), 8192);
            out = new BufferedOutputStream(socket.getOutputStream(), 8192);
        } catch (IOException e) {
            throw new RedisConnectionException("cannot connect to redis at " + host + ":" + port, e);
        }
    }

    public Object call(String... args) {
        try {
            writeCommand(args);
            out.flush();
            return readReply();
        } catch (IOException e) {
            broken = true;
            throw new RedisConnectionException("redis I/O error: " + e.getMessage(), e);
        }
    }

    public boolean isBroken() {
        return broken || socket.isClosed();
    }

    @Override
    public void close() {
        broken = true;
        try {
            socket.close();
        } catch (IOException ignored) {
            // nothing useful to do
        }
    }

    // ---- RESP encoding ---------------------------------------------------------------------

    private void writeCommand(String[] args) throws IOException {
        out.write('*');
        writeAsciiLong(args.length);
        out.write(CRLF);
        for (String arg : args) {
            byte[] b = arg.getBytes(StandardCharsets.UTF_8);
            out.write('$');
            writeAsciiLong(b.length);
            out.write(CRLF);
            out.write(b);
            out.write(CRLF);
        }
    }

    private void writeAsciiLong(long v) throws IOException {
        out.write(Long.toString(v).getBytes(StandardCharsets.US_ASCII));
    }

    // ---- RESP decoding ---------------------------------------------------------------------

    private Object readReply() throws IOException {
        int type = in.read();
        if (type == -1) {
            throw new EOFException("connection closed by redis");
        }
        switch (type) {
            case '+':
                return readLine();
            case '-':
                throw new RedisReplyException(readLine());
            case ':':
                return Long.parseLong(readLine());
            case '$': {
                int len = Integer.parseInt(readLine());
                if (len < 0) {
                    return null;
                }
                byte[] data = in.readNBytes(len);
                if (data.length != len) {
                    throw new EOFException("truncated bulk reply");
                }
                expectCrlf();
                return new String(data, StandardCharsets.UTF_8);
            }
            case '*': {
                int n = Integer.parseInt(readLine());
                if (n < 0) {
                    return null;
                }
                List<Object> items = new ArrayList<>(n);
                for (int i = 0; i < n; i++) {
                    items.add(readReply());
                }
                return items;
            }
            default:
                throw new IOException("unknown RESP type byte: " + (char) type);
        }
    }

    private String readLine() throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream(32);
        while (true) {
            int b = in.read();
            if (b == -1) {
                throw new EOFException("connection closed mid-reply");
            }
            if (b == '\r') {
                int lf = in.read();
                if (lf != '\n') {
                    throw new IOException("malformed RESP line ending");
                }
                return buf.toString(StandardCharsets.UTF_8);
            }
            buf.write(b);
        }
    }

    private void expectCrlf() throws IOException {
        if (in.read() != '\r' || in.read() != '\n') {
            throw new IOException("malformed RESP bulk terminator");
        }
    }
}
