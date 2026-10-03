package com.github.aarnavnaga.kvstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Integration tests against a live cluster: set KVSTORE_SERVERS=host:port,... (see
 * client-java/run-it.sh, which starts one in Docker and runs these).
 */
@EnabledIfEnvironmentVariable(named = "KVSTORE_SERVERS", matches = ".+")
class KvClientIT {
  private static List<String> servers;
  private static KvClient kv;

  @BeforeAll
  static void connect() {
    servers = List.of(System.getenv("KVSTORE_SERVERS").split(","));
    kv = KvClient.builder().servers(servers).operationTimeout(Duration.ofSeconds(20)).build();
  }

  @AfterAll
  static void close() {
    kv.close();
  }

  private static String unique(String prefix) {
    return prefix + "-" + UUID.randomUUID();
  }

  @Test
  void putGetDeleteRoundTrip() {
    String key = unique("rt");
    assertEquals(Optional.empty(), kv.get(key));
    kv.put(key, "hello");
    assertEquals(Optional.of("hello"), kv.getString(key));
    kv.put(key, "world");
    assertEquals(Optional.of("world"), kv.getString(key));
    kv.delete(key);
    assertEquals(Optional.empty(), kv.get(key));
  }

  @Test
  void discoversTheLeaderFromAnySingleNode() {
    for (String s : servers) {
      try (KvClient one = KvClient.builder().servers(s).build()) {
        String key = unique("via-" + s);
        one.put(key, "v");
        assertEquals(Optional.of("v"), one.getString(key));
        assertNotNull(one.leader(), "leader should be known after a successful write");
      }
    }
  }

  @Test
  void largeValuesRoundTrip() {
    byte[] big = new byte[1 << 20];
    for (int i = 0; i < big.length; i++) {
      big[i] = (byte) (i * 31);
    }
    String key = unique("big");
    kv.put(key, big);
    assertArrayEquals(big, kv.get(key).orElseThrow());
  }

  @Test
  void manyThreadsShareOneClient() throws Exception {
    int threads = 16;
    int perThread = 200;
    String prefix = unique("mt");
    List<Thread> ts = new ArrayList<>();
    AtomicInteger errors = new AtomicInteger();
    for (int t = 0; t < threads; t++) {
      int id = t;
      ts.add(
          Thread.ofVirtual()
              .start(
                  () -> {
                    for (int i = 0; i < perThread; i++) {
                      String k = prefix + "-" + id + "-" + i;
                      try {
                        kv.put(k, Integer.toString(i));
                        if (!kv.getString(k).orElse("").equals(Integer.toString(i))) {
                          errors.incrementAndGet();
                        }
                      } catch (KvException e) {
                        errors.incrementAndGet();
                      }
                    }
                  }));
    }
    for (Thread t : ts) {
      t.join();
    }
    assertEquals(0, errors.get());
  }

  /**
   * Writes continuously while the harness kills the leader and restarts it; every
   * acknowledged write must be readable afterwards. Coordinates with run-it.sh through
   * files in KVSTORE_FAULT_DIR.
   */
  @Test
  @EnabledIfEnvironmentVariable(named = "KVSTORE_FAULT_DIR", matches = ".+")
  void acknowledgedWritesSurviveLeaderFailover() throws Exception {
    Path dir = Path.of(System.getenv("KVSTORE_FAULT_DIR"));
    Map<String, String> acked = new ConcurrentHashMap<>();
    AtomicInteger unknown = new AtomicInteger();
    AtomicBoolean stop = new AtomicBoolean();
    String prefix = unique("fo");
    List<Thread> writers = new ArrayList<>();
    for (int w = 0; w < 4; w++) {
      int id = w;
      writers.add(
          Thread.ofVirtual()
              .start(
                  () -> {
                    for (int i = 0; !stop.get(); i++) {
                      String k = prefix + "-" + id + "-" + i;
                      try {
                        kv.put(k, k);
                        acked.put(k, k);
                      } catch (KvTimeoutException e) {
                        unknown.incrementAndGet();
                      }
                    }
                  }));
    }
    Thread.sleep(2000);
    Files.createFile(dir.resolve("kill-leader"));
    waitFor(dir.resolve("leader-restarted"), Duration.ofSeconds(60));
    Thread.sleep(2000);
    stop.set(true);
    for (Thread t : writers) {
      t.join();
    }

    assertTrue(acked.size() > 100, "too few writes: " + acked.size());
    for (Map.Entry<String, String> e : acked.entrySet()) {
      assertEquals(Optional.of(e.getValue()), kv.getString(e.getKey()), "lost " + e.getKey());
    }
    System.out.printf(
        "failover: %d acknowledged writes verified, %d outcomes unknown%n", acked.size(), unknown.get());
  }

  private static void waitFor(Path file, Duration timeout) throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (!Files.exists(file)) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("harness never created " + file);
      }
      Thread.sleep(100);
    }
  }
}
