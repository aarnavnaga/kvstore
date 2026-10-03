package com.github.aarnavnaga.kvstore;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;

/**
 * One multiplexed TCP (or TLS) connection to a node. Any number of threads may have
 * requests in flight at once: each request carries an id, a reader thread routes each
 * response to the future waiting for that id, and writes are serialized.
 *
 * <p>A connection that fails is closed for good and every pending request fails with
 * the I/O error; the pool replaces it on the next use.
 */
final class Connection implements AutoCloseable {
  private final String addr;
  private final Socket socket;
  private final OutputStream out;
  private final DataInputStream in;
  private final Map<Long, CompletableFuture<Wire.Frame>> pending = new ConcurrentHashMap<>();
  private final AtomicLong nextId = new AtomicLong(1);
  private final Thread reader;
  private volatile boolean closed;

  private Connection(String addr, Socket socket) throws IOException {
    this.addr = addr;
    this.socket = socket;
    this.out = new BufferedOutputStream(socket.getOutputStream(), 64 * 1024);
    this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 64 * 1024));
    this.reader = Thread.ofVirtual().name("kvstore-reader-" + addr).unstarted(this::readLoop);
  }

  static Connection open(String addr, int connectTimeoutMs, SSLContext tls) throws IOException {
    int colon = addr.lastIndexOf(':');
    if (colon <= 0) {
      throw new IllegalArgumentException("address must be host:port, got " + addr);
    }
    InetSocketAddress target =
        new InetSocketAddress(addr.substring(0, colon), Integer.parseInt(addr.substring(colon + 1)));
    Socket raw = new Socket();
    try {
      raw.setTcpNoDelay(true);
      raw.setKeepAlive(true);
      raw.connect(target, connectTimeoutMs);
      Socket s = raw;
      if (tls != null) {
        SSLSocket ssl =
            (SSLSocket) tls.getSocketFactory().createSocket(raw, target.getHostString(), target.getPort(), true);
        ssl.setEnabledProtocols(new String[] {"TLSv1.3"});
        ssl.setSoTimeout(connectTimeoutMs);
        ssl.startHandshake();
        ssl.setSoTimeout(0);
        s = ssl;
      }
      Connection c = new Connection(addr, s);
      c.reader.start();
      return c;
    } catch (IOException | RuntimeException e) {
      raw.close();
      throw e;
    }
  }

  /** Sends a request; the future completes with the response frame or the I/O failure. */
  CompletableFuture<Wire.Frame> call(byte type, byte[] payload) {
    CompletableFuture<Wire.Frame> f = new CompletableFuture<>();
    if (closed) {
      f.completeExceptionally(new IOException("connection to " + addr + " is closed"));
      return f;
    }
    long id = nextId.getAndIncrement();
    pending.put(id, f);
    byte[] frame = Wire.encodeFrame(type, id, payload);
    try {
      synchronized (out) {
        out.write(frame);
        out.flush();
      }
    } catch (IOException e) {
      fail(e);
    }
    return f;
  }

  boolean isOpen() {
    return !closed;
  }

  @Override
  public void close() {
    fail(new IOException("connection to " + addr + " closed by client"));
  }

  private void readLoop() {
    byte[] header = new byte[Wire.HEADER];
    try {
      while (!closed) {
        in.readFully(header);
        ByteBuffer h = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN);
        int length = h.getInt();
        if (length < 9 || length > Wire.MAX_FRAME) {
          throw new IOException("bad frame length " + length + " from " + addr);
        }
        byte type = h.get();
        long id = h.getLong();
        byte[] payload = new byte[length - 9];
        in.readFully(payload);
        CompletableFuture<Wire.Frame> f = pending.remove(id);
        if (f != null) {
          f.complete(new Wire.Frame(type, id, payload));
        }
      }
    } catch (EOFException e) {
      fail(new IOException("connection to " + addr + " closed by server", e));
    } catch (IOException e) {
      fail(e);
    }
  }

  private void fail(IOException cause) {
    closed = true;
    try {
      socket.close();
    } catch (IOException ignored) {
      // already failing
    }
    for (Long id : pending.keySet()) {
      CompletableFuture<Wire.Frame> f = pending.remove(id);
      if (f != null) {
        f.completeExceptionally(cause);
      }
    }
  }
}
