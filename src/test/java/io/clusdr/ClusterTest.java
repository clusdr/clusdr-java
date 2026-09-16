package io.clusdr;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ClusterTest {
  private static FakeDaemon fake;

  @BeforeAll
  static void start() {
    fake = FakeDaemon.start();
  }

  @AfterAll
  static void stop() {
    fake.stop();
  }

  @Test
  void membersLeaderWatchPublish() {
    try (Cluster c = Clusdr.dial(fake.addr, Options.defaults().insecure(true))) {
      List<Member> members = c.members();
      assertEquals(1, members.size());
      assertEquals("node-a", members.get(0).id());
      assertTrue(members.get(0).leader());

      Member leader = c.leader();
      assertEquals("node-a", leader.id());
      assertTrue(leader.leader());

      Watch w = c.watch();
      List<Event> events = new CopyOnWriteArrayList<>();
      Thread reader = readUntil(w, events, "custom.deployment");
      waitFor(events, "member.join", 3000);
      c.publish("deployment", Map.of("sha", "abc"));
      joinWatch(reader, w, 3000);
      assertTrue(events.stream().anyMatch(e -> e.type().equals("member.join") && e.source().equals("node-a")));
      Event custom = events.stream().filter(e -> e.type().equals("custom.deployment")).findFirst().orElse(null);
      assertTrue(custom != null);
      assertEquals("{\"sha\":\"abc\"}", new String(custom.payload(), StandardCharsets.UTF_8));
      assertTrue(fake.state.published.stream().anyMatch(p -> p.getTopic().equals("deployment")));
    }
  }

  @Test
  void watchTopics() {
    try (Cluster c = Clusdr.dial(fake.addr, Options.defaults().insecure(true))) {
      Watch w = c.watch(WatchFilter.all().topics("deployment"));
      List<Event> events = new CopyOnWriteArrayList<>();
      Thread reader = readUntil(w, events, "custom.deployment");
      sleep(50);
      c.publish("noise", "x");
      c.publish("deployment", Map.of("sha", "abc"));
      joinWatch(reader, w, 3000);
      List<String> types = events.stream().map(Event::type).toList();
      assertFalse(types.contains("member.join"));
      assertFalse(types.contains("custom.noise"));
      assertTrue(types.contains("custom.deployment"));
    }
  }

  @Test
  void watchEventTypes() {
    try (Cluster c = Clusdr.dial(fake.addr, Options.defaults().insecure(true))) {
      Watch w = c.watch(WatchFilter.all().eventTypes("custom.deployment"));
      List<Event> events = new CopyOnWriteArrayList<>();
      Thread reader = readUntil(w, events, "custom.deployment");
      sleep(50);
      c.publish("noise", "x");
      c.publish("deployment", Map.of("sha", "abc"));
      joinWatch(reader, w, 3000);
      List<String> types = events.stream().map(Event::type).toList();
      assertFalse(types.contains("member.join"));
      assertFalse(types.contains("custom.noise"));
    }
  }

  @Test
  void watchBadTopic() {
    try (Cluster c = Clusdr.dial(fake.addr, Options.defaults().insecure(true))) {
      assertThrows(ClusdrException.class, () -> c.watch(WatchFilter.all().topics("bad topic")));
    }
  }

  @Test
  void retryUntilReady() {
    FakeDaemon.FlakyHealth health = new FakeDaemon.FlakyHealth(2);
    FakeDaemon flaky = new FakeDaemon(health);
    try (Cluster c =
        Clusdr.dial(flaky.addr, Options.defaults().insecure(true).readyTimeout(Duration.ofSeconds(5)))) {
      assertEquals("node-a", c.members().get(0).id());
      assertTrue(health.calls.get() >= 3);
    } finally {
      flaky.stop();
    }
  }

  @Test
  void emptyDial() {
    ClusdrException err =
        assertThrows(ClusdrException.class, () -> Clusdr.dial("  ", Options.defaults().insecure(true)));
    assertTrue(err.getMessage().contains("empty"));
  }

  @Test
  void publishTooLarge() {
    try (Cluster c = Clusdr.dial(fake.addr, Options.defaults().insecure(true))) {
      byte[] big = new byte[Options.MAX_PAYLOAD + 1];
      ClusdrException err = assertThrows(ClusdrException.class, () -> c.publish("deployment", big));
      assertTrue(err.getMessage().contains("exceeds"));
    }
  }

  @Test
  void encodePayload() {
    assertArrayEquals(new byte[0], Payloads.encode(null));
    assertArrayEquals("raw".getBytes(StandardCharsets.UTF_8), Payloads.encode("raw".getBytes(StandardCharsets.UTF_8)));
    assertArrayEquals("hi".getBytes(StandardCharsets.UTF_8), Payloads.encode("hi"));
    assertArrayEquals("{\"sha\":\"abc\"}".getBytes(StandardCharsets.UTF_8), Payloads.encode(Map.of("sha", "abc")));
    assertThrows(IllegalArgumentException.class, () -> Payloads.encode(1));
  }

  private static Thread readUntil(Watch w, List<Event> events, String stopType) {
    Thread t =
        new Thread(
            () -> {
              for (Event ev : w) {
                events.add(ev);
                if (ev.type().equals(stopType)) {
                  break;
                }
              }
            });
    t.start();
    return t;
  }

  private static void waitFor(List<Event> events, String type, long timeoutMs) {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      if (events.stream().anyMatch(e -> e.type().equals(type))) {
        return;
      }
      sleep(20);
    }
    throw new AssertionError("timed out waiting for " + type);
  }

  private static void joinWatch(Thread reader, Watch w, long timeoutMs) {
    try {
      reader.join(timeoutMs);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new AssertionError(ie);
    }
    if (reader.isAlive()) {
      w.close();
      throw new AssertionError("watch timeout");
    }
    w.close();
  }

  private static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new AssertionError(ie);
    }
  }
}
