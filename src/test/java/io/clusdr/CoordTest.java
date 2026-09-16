package io.clusdr;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class CoordTest {
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
  void lockAcquireUnlock() {
    Cluster a = Clusdr.dial(fake.addr, Options.defaults().insecure(true).holder("worker-a"));
    Cluster b = Clusdr.dial(fake.addr, Options.defaults().insecure(true).holder("worker-b"));
    try {
      Lock lk = a.lock("scheduler", Duration.ofSeconds(1));
      assertTrue(lk.token() > 0);
      assertEquals("worker-a", lk.holder());
      assertTrue(lk.deadline() != null);

      assertTrue(b.tryLock("scheduler", Duration.ofSeconds(1)).isEmpty());

      a.unlock("scheduler");
      synchronized (fake.state.coord) {
        assertFalse(fake.state.coord.locks.containsKey("scheduler"));
      }

      Optional<Lock> won = b.tryLock("scheduler", Duration.ofSeconds(1));
      assertTrue(won.isPresent());
      assertTrue(won.get().token() > lk.token());
      b.unlock("scheduler");
    } finally {
      a.close();
      b.close();
    }
  }

  @Test
  void lockWaitsForUnlock() {
    Cluster a = Clusdr.dial(fake.addr, Options.defaults().insecure(true).holder("worker-a"));
    Cluster b = Clusdr.dial(fake.addr, Options.defaults().insecure(true).holder("worker-b"));
    try {
      Optional<Lock> first = a.tryLock("job", Duration.ofSeconds(1));
      assertTrue(first.isPresent());
      Thread waiter =
          new Thread(
              () -> {
                Lock got = b.lock("job", Duration.ofSeconds(1), Duration.ofSeconds(3));
                assertEquals("worker-b", got.holder());
              });
      waiter.start();
      try {
        Thread.sleep(50);
        a.unlock("job");
        waiter.join(3000);
        assertFalse(waiter.isAlive());
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new AssertionError(ie);
      }
    } finally {
      a.close();
      b.close();
    }
  }

  @Test
  void unlockRequiresAcquire() {
    try (Cluster c = Clusdr.dial(fake.addr, Options.defaults().insecure(true))) {
      ClusdrException err = assertThrows(ClusdrException.class, () -> c.unlock("missing"));
      assertTrue(err.getMessage().contains("not held"));
    }
  }

  @Test
  void closeReleasesLock() {
    Cluster a = Clusdr.dial(fake.addr, Options.defaults().insecure(true).holder("worker-a"));
    a.lock("job", Duration.ofSeconds(1));
    a.close();
    synchronized (fake.state.coord) {
      assertFalse(fake.state.coord.locks.containsKey("job"));
    }
  }

  @Test
  void lockRenewKeepsGrant() throws Exception {
    try (Cluster c = Clusdr.dial(fake.addr, Options.defaults().insecure(true).holder("worker-a"))) {
      Lock lk = c.lock("job", Duration.ofMillis(150));
      long first;
      synchronized (fake.state.coord) {
        first = fake.state.coord.locks.get("job").deadline;
      }
      Thread.sleep(200);
      fake.state.coord.expireDue();
      synchronized (fake.state.coord) {
        assertTrue(fake.state.coord.locks.containsKey("job"));
        assertTrue(fake.state.coord.locks.get("job").deadline > first);
      }
      assertTrue(lk.token() > 0);
    }
  }

  @Test
  void leaseGrantRevoke() {
    Cluster a = Clusdr.dial(fake.addr, Options.defaults().insecure(true).holder("worker-a"));
    Cluster b = Clusdr.dial(fake.addr, Options.defaults().insecure(true).holder("worker-b"));
    try {
      Lease ls = a.lease("worker-1", Duration.ofSeconds(1));
      assertTrue(ls.token() > 0);
      assertEquals("worker-a", ls.owner());
      ClusdrException held = assertThrows(ClusdrException.class, () -> b.lease("worker-1", Duration.ofSeconds(1)));
      assertTrue(held.getMessage().contains("held"));
      a.revoke("worker-1");
      synchronized (fake.state.coord) {
        assertFalse(fake.state.coord.leases.containsKey("worker-1"));
      }
      Lease won = b.lease("worker-1", Duration.ofSeconds(1));
      assertTrue(won.token() > ls.token());
      b.revoke("worker-1");
    } finally {
      a.close();
      b.close();
    }
  }

  @Test
  void leaseRenewAndCancel() throws Exception {
    try (Cluster c = Clusdr.dial(fake.addr, Options.defaults().insecure(true).holder("worker-a"))) {
      Lease ls = c.lease("worker-1", Duration.ofMillis(80));
      long first;
      synchronized (fake.state.coord) {
        first = fake.state.coord.leases.get("worker-1").deadline;
      }
      c.renew("worker-1");
      synchronized (fake.state.coord) {
        assertTrue(fake.state.coord.leases.get("worker-1").deadline > first);
      }
      ls.stopRenew();
      long deadline = System.currentTimeMillis() + 2000;
      boolean expired = false;
      while (System.currentTimeMillis() < deadline) {
        fake.state.coord.expireDue();
        synchronized (fake.state.coord) {
          if (!fake.state.coord.leases.containsKey("worker-1")) {
            expired = true;
            break;
          }
        }
        Thread.sleep(10);
      }
      assertTrue(expired);
      assertTrue(ls.token() > 0);
    }
  }

  @Test
  void closeRevokesLease() {
    Cluster c = Clusdr.dial(fake.addr, Options.defaults().insecure(true).holder("worker-a"));
    c.lease("worker-1", Duration.ofSeconds(1));
    c.close();
    synchronized (fake.state.coord) {
      assertFalse(fake.state.coord.leases.containsKey("worker-1"));
    }
  }

  @Test
  void revokeRequiresGrant() {
    try (Cluster c = Clusdr.dial(fake.addr, Options.defaults().insecure(true))) {
      assertTrue(assertThrows(ClusdrException.class, () -> c.revoke("missing")).getMessage().contains("not held"));
      assertTrue(assertThrows(ClusdrException.class, () -> c.renew("missing")).getMessage().contains("not held"));
    }
  }

  @Test
  void clusdrException() {
    ClusdrException err = new ClusdrException("clusdr: x");
    assertEquals("clusdr: x", err.getMessage());
    assertTrue(err instanceof RuntimeException);
  }
}
