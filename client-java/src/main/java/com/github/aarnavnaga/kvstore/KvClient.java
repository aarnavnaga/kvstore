package com.github.aarnavnaga.kvstore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;

/**
 * Client for a kvstore cluster. Thread-safe; share one instance.
 *
 * <ul>
 *   <li><b>Leader discovery.</b> Requests go to the last known leader. A follower answers
 *       NotLeader with the leader's address and the client retries there at once; with no
 *       hint (an election in progress) it tries the next node after a backoff.
 *   <li><b>Retries with backoff.</b> Connection failures, timeouts and leaderless periods
 *       are retried until the operation deadline, with exponential backoff and full jitter
 *       so many clients do not retry in lockstep.
 *   <li><b>Exactly-once writes.</b> Every write carries a session (id, sequence number),
 *       and retries reuse the number, so the cluster applies a retried write once. The
 *       server's deduplication assumes a session has one write in flight at a time, so
 *       concurrent writes each check out their own session from a pool of lanes.
 *   <li><b>Connection pooling.</b> A few multiplexed connections per node carry every
 *       thread's requests concurrently.
 * </ul>
 *
 * <pre>{@code
 * try (KvClient kv = KvClient.builder().servers("node1:7000", "node2:7000", "node3:7000").build()) {
 *   kv.put("greeting", "hello");
 *   Optional<String> v = kv.getString("greeting");
 * }
 * }</pre>
 */
public final class KvClient implements AutoCloseable {
  private final List<String> servers;
  private final Duration requestTimeout;
  private final Duration operationTimeout;
  private final Duration backoffBase;
  private final Duration backoffMax;
  private final ConnectionPool pool;
  private final BlockingQueue<Lane> lanes;
  private final AtomicInteger nextServer = new AtomicInteger();
  private volatile String leader;

  private KvClient(Builder b) {
    if (b.servers.isEmpty()) {
      throw new IllegalArgumentException("at least one server is required");
    }
    this.servers = List.copyOf(b.servers);
    this.requestTimeout = b.requestTimeout;
    this.operationTimeout = b.operationTimeout;
    this.backoffBase = b.backoffBase;
    this.backoffMax = b.backoffMax;
    this.pool = new ConnectionPool(b.connectionsPerNode, (int) b.connectTimeout.toMillis(), b.tls);
    this.lanes = new ArrayBlockingQueue<>(b.maxConcurrentWrites);
    SecureRandom r = new SecureRandom();
    for (int i = 0; i < b.maxConcurrentWrites; i++) {
      long id = 0;
      while (id == 0) {
        id = r.nextLong() & Long.MAX_VALUE;
      }
      lanes.add(new Lane(id));
    }
  }

  /**
   * A write session. The server remembers, per session id, the highest sequence number it
   * applied, and treats anything at or below it as a retry to acknowledge but not apply.
   * Two writes in flight on one session could therefore be reordered into a silent loss
   * (seq 6 commits, then seq 5 is "a duplicate"); a lane is only ever used by one write
   * at a time, from first attempt to last retry.
   */
  private static final class Lane {
    final long id;
    long nextSeq = 1;

    Lane(long id) {
      this.id = id;
    }
  }

  private Lane acquireLane() {
    try {
      return lanes.take();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new KvException(StatusCode.UNAVAILABLE, "interrupted", e);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public void put(String key, byte[] value) {
    Lane lane = acquireLane();
    try {
      execute(Wire.PUT, Wire.encodePut(lane.id, lane.nextSeq++, key, value));
    } finally {
      lanes.add(lane);
    }
  }

  public void put(String key, String value) {
    put(key, value.getBytes(StandardCharsets.UTF_8));
  }

  /** Returns the value, or empty if the key does not exist. Linearizable. */
  public Optional<byte[]> get(String key) {
    Wire.Response r = execute(Wire.GET, Wire.encodeGet(key));
    return r.code() == StatusCode.NOT_FOUND ? Optional.empty() : Optional.of(r.value());
  }

  public Optional<String> getString(String key) {
    return get(key).map(v -> new String(v, StandardCharsets.UTF_8));
  }

  public void delete(String key) {
    Lane lane = acquireLane();
    try {
      execute(Wire.DELETE, Wire.encodeDelete(lane.id, lane.nextSeq++, key));
    } finally {
      lanes.add(lane);
    }
  }

  /** The node the client currently believes is leader, or null if unknown. */
  public String leader() {
    return leader;
  }

  @Override
  public void close() {
    pool.close();
  }

  private Wire.Response execute(byte type, byte[] payload) {
    long deadline = System.nanoTime() + operationTimeout.toNanos();
    int attempt = 0;
    Throwable last = null;
    while (System.nanoTime() < deadline) {
      String target = leader != null ? leader : servers.get(Math.floorMod(nextServer.get(), servers.size()));
      Wire.Response resp;
      try {
        Wire.Frame f =
            pool.get(target).call(type, payload).get(requestTimeout.toMillis(), TimeUnit.MILLISECONDS);
        resp = Wire.decodeResponse(f.payload());
      } catch (IOException | ExecutionException | TimeoutException e) {
        // Unreachable, broken or silent: drop its connections and move on.
        pool.evict(target);
        last = e;
        forgetLeaderAndAdvance(target);
        backoff(attempt++, deadline);
        continue;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new KvException(StatusCode.UNAVAILABLE, "interrupted", e);
      }

      switch (resp.code()) {
        case OK, NOT_FOUND -> {
          leader = target;
          return resp;
        }
        case NOT_LEADER -> {
          if (!resp.leaderHint().isEmpty() && !resp.leaderHint().equals(target)) {
            leader = resp.leaderHint();  // go straight to the hinted leader, no backoff
            continue;
          }
          forgetLeaderAndAdvance(target);
          backoff(attempt++, deadline);
        }
        case TIMEOUT, UNAVAILABLE -> {
          last = new KvException(resp.code(), new String(resp.value(), StandardCharsets.UTF_8));
          forgetLeaderAndAdvance(target);
          backoff(attempt++, deadline);
        }
        default -> throw new KvException(resp.code(), new String(resp.value(), StandardCharsets.UTF_8));
      }
    }
    throw new KvTimeoutException("no successful response within " + operationTimeout, last);
  }

  private void forgetLeaderAndAdvance(String failed) {
    if (failed.equals(leader)) {
      leader = null;
    }
    nextServer.incrementAndGet();
  }

  private void backoff(int attempt, long deadlineNanos) {
    // Full jitter: sleep a uniform random time up to the capped exponential bound.
    long capMs = Math.min(backoffMax.toMillis(), backoffBase.toMillis() << Math.min(attempt, 20));
    long sleepMs = ThreadLocalRandom.current().nextLong(capMs + 1);
    long leftMs = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
    try {
      Thread.sleep(Math.max(0, Math.min(sleepMs, leftMs)));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Configuration for {@link KvClient}. */
  public static final class Builder {
    private List<String> servers = List.of();
    private Duration connectTimeout = Duration.ofSeconds(1);
    private Duration requestTimeout = Duration.ofSeconds(2);
    private Duration operationTimeout = Duration.ofSeconds(10);
    private Duration backoffBase = Duration.ofMillis(20);
    private Duration backoffMax = Duration.ofMillis(500);
    private int connectionsPerNode = 2;
    private int maxConcurrentWrites = 64;
    private SSLContext tls;

    /** Any subset of the cluster's node addresses ("host:port"). */
    public Builder servers(String... servers) {
      this.servers = List.of(servers);
      return this;
    }

    public Builder servers(List<String> servers) {
      this.servers = List.copyOf(servers);
      return this;
    }

    public Builder connectTimeout(Duration d) {
      this.connectTimeout = d;
      return this;
    }

    /** How long to wait for one node to answer one attempt. */
    public Builder requestTimeout(Duration d) {
      this.requestTimeout = d;
      return this;
    }

    /** Total time an operation may take across retries before KvTimeoutException. */
    public Builder operationTimeout(Duration d) {
      this.operationTimeout = d;
      return this;
    }

    public Builder backoff(Duration base, Duration max) {
      this.backoffBase = base;
      this.backoffMax = max;
      return this;
    }

    /** Writes that may be in flight at once; more wait for a free session lane. */
    public Builder maxConcurrentWrites(int n) {
      if (n < 1) {
        throw new IllegalArgumentException("maxConcurrentWrites must be at least 1");
      }
      this.maxConcurrentWrites = n;
      return this;
    }

    public Builder connectionsPerNode(int n) {
      if (n < 1) {
        throw new IllegalArgumentException("connectionsPerNode must be at least 1");
      }
      this.connectionsPerNode = n;
      return this;
    }

    /**
     * Mutual TLS for clusters started with --tls_*: an SSLContext whose key manager holds
     * a certificate from the cluster CA and whose trust manager trusts that CA.
     */
    public Builder tls(SSLContext tls) {
      this.tls = tls;
      return this;
    }

    public KvClient build() {
      return new KvClient(this);
    }
  }
}
