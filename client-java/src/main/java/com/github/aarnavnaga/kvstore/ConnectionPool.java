package com.github.aarnavnaga.kvstore;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;

/**
 * A fixed number of multiplexed connections per node, opened lazily and replaced when
 * they fail. Requests are spread round-robin over a node's connections.
 */
final class ConnectionPool implements AutoCloseable {
  private final int perNode;
  private final int connectTimeoutMs;
  private final SSLContext tls;
  private final Map<String, Slot[]> nodes = new ConcurrentHashMap<>();
  private final AtomicInteger next = new AtomicInteger();

  private static final class Slot {
    Connection conn;
  }

  ConnectionPool(int perNode, int connectTimeoutMs, SSLContext tls) {
    this.perNode = perNode;
    this.connectTimeoutMs = connectTimeoutMs;
    this.tls = tls;
  }

  Connection get(String addr) throws IOException {
    Slot[] slots =
        nodes.computeIfAbsent(
            addr,
            a -> {
              Slot[] s = new Slot[perNode];
              for (int i = 0; i < perNode; i++) {
                s[i] = new Slot();
              }
              return s;
            });
    Slot slot = slots[Math.floorMod(next.getAndIncrement(), perNode)];
    synchronized (slot) {
      if (slot.conn == null || !slot.conn.isOpen()) {
        slot.conn = Connection.open(addr, connectTimeoutMs, tls);
      }
      return slot.conn;
    }
  }

  /** Closes every connection to addr, e.g. after it stopped answering. */
  void evict(String addr) {
    Slot[] slots = nodes.get(addr);
    if (slots == null) {
      return;
    }
    for (Slot slot : slots) {
      synchronized (slot) {
        if (slot.conn != null) {
          slot.conn.close();
          slot.conn = null;
        }
      }
    }
  }

  @Override
  public void close() {
    for (String addr : nodes.keySet()) {
      evict(addr);
    }
  }
}
