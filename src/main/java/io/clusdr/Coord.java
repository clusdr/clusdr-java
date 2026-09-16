package io.clusdr;

import io.clusdr.v1alpha1.GrantLeaseRequest;
import io.clusdr.v1alpha1.GrantLeaseResponse;
import io.clusdr.v1alpha1.LeaseServiceGrpc;
import io.clusdr.v1alpha1.LockRequest;
import io.clusdr.v1alpha1.LockResponse;
import io.clusdr.v1alpha1.LockServiceGrpc;
import io.clusdr.v1alpha1.RenewLeaseRequest;
import io.clusdr.v1alpha1.RenewLockRequest;
import io.clusdr.v1alpha1.RevokeLeaseRequest;
import io.clusdr.v1alpha1.UnlockRequest;
import io.grpc.Status;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

final class Coord {
  private final Cluster host;
  private final Object mu = new Object();
  private final Map<String, Lock> held = new HashMap<>();
  private final Map<String, Lease> leased = new HashMap<>();
  private final Map<String, ScheduledFuture<?>> renewals = new HashMap<>();

  Coord(Cluster host) {
    this.host = host;
  }

  Lock lock(String name, Duration ttl, Duration timeout) {
    Lock existing = heldLock(name);
    if (existing != null) {
      return existing;
    }
    long deadline = host.deadline(timeout);
    LockResponse resp;
    try {
      resp =
          Retry.retry(
              () ->
                  lockStub(deadline)
                      .lock(
                          LockRequest.newBuilder()
                              .setName(name)
                              .setHolder(host.holder())
                              .setTtlMs(Grants.ttlMs(ttl))
                              .build()),
              deadline,
              Retry::isTransient,
              host::closed);
    } catch (RuntimeException err) {
      throw ClusdrException.wrap("clusdr: lock " + json(name), err);
    }
    if (!resp.getAcquired()) {
      String msg = resp.getMessage().isEmpty() ? "not acquired" : resp.getMessage();
      throw new ClusdrException("clusdr: lock " + json(name) + ": " + msg);
    }
    return adoptLock(resp, name, ttl);
  }

  Optional<Lock> tryLock(String name, Duration ttl, Duration timeout) {
    Lock existing = heldLock(name);
    if (existing != null) {
      return Optional.of(existing);
    }
    long deadline = host.deadline(timeout);
    LockResponse resp;
    try {
      resp =
          Retry.retry(
              () ->
                  lockStub(deadline)
                      .tryLock(
                          LockRequest.newBuilder()
                              .setName(name)
                              .setHolder(host.holder())
                              .setTtlMs(Grants.ttlMs(ttl))
                              .build()),
              deadline,
              Retry::isTransient,
              host::closed);
    } catch (RuntimeException err) {
      throw ClusdrException.wrap("clusdr: trylock " + json(name), err);
    }
    if (!resp.getAcquired()) {
      return Optional.empty();
    }
    return Optional.of(adoptLock(resp, name, ttl));
  }

  void unlock(String name, Duration timeout) {
    Lock lk = heldLock(name);
    if (lk == null) {
      throw new ClusdrException("clusdr: lock " + json(name) + " is not held by this client");
    }
    releaseLock(lk, timeout);
  }

  Lease lease(String name, Duration ttl, Duration timeout) {
    Lease existing = heldLease(name);
    if (existing != null) {
      return existing;
    }
    long deadline = host.deadline(timeout);
    GrantLeaseResponse resp;
    try {
      resp =
          Retry.retry(
              () ->
                  leaseStub(deadline)
                      .grant(
                          GrantLeaseRequest.newBuilder()
                              .setName(name)
                              .setOwner(host.holder())
                              .setTtlMs(Grants.ttlMs(ttl))
                              .build()),
              deadline,
              Retry::isTransient,
              host::closed);
    } catch (RuntimeException err) {
      throw ClusdrException.wrap("clusdr: lease " + json(name), err);
    }
    if (!resp.getGranted()) {
      String msg = resp.getMessage().isEmpty() ? "not granted" : resp.getMessage();
      if (!resp.getOwner().isEmpty()) {
        throw new ClusdrException(
            "clusdr: lease " + json(name) + ": " + msg + " (owner " + resp.getOwner() + ")");
      }
      throw new ClusdrException("clusdr: lease " + json(name) + ": " + msg);
    }
    return adoptLease(resp, name, ttl);
  }

  void renew(String name, Duration timeout) {
    Lease ls = heldLease(name);
    if (ls == null) {
      throw new ClusdrException("clusdr: lease " + json(name) + " is not held by this client");
    }
    long deadline = host.deadline(timeout);
    try {
      var resp =
          Retry.retry(
              () ->
                  leaseStub(deadline)
                      .renew(
                          RenewLeaseRequest.newBuilder()
                              .setName(ls.name())
                              .setOwner(ls.owner())
                              .setFencingToken(ls.token())
                              .setTtlMs(0)
                              .build()),
              deadline,
              Retry::isTransient,
              host::closed);
      if (resp != null && !resp.getRenewed()) {
        throw new ClusdrException("clusdr: renew " + json(name) + ": " + resp.getMessage());
      }
      if (resp != null && resp.getDeadlineUnixMs() != 0) {
        ls.setDeadline(Grants.fromMs(resp.getDeadlineUnixMs()));
      }
    } catch (RuntimeException err) {
      throw ClusdrException.wrap("clusdr: renew " + json(name), err);
    }
  }

  void revoke(String name, Duration timeout) {
    Lease ls = heldLease(name);
    if (ls == null) {
      throw new ClusdrException("clusdr: lease " + json(name) + " is not held by this client");
    }
    dropLease(ls, timeout);
  }

  void releaseGrants() {
    List<Lock> locks;
    List<Lease> leases;
    synchronized (mu) {
      locks = new ArrayList<>(held.values());
      leases = new ArrayList<>(leased.values());
    }
    for (Lock lk : locks) {
      try {
        releaseLock(lk, null);
      } catch (RuntimeException ignored) {
        // best-effort on close
      }
    }
    for (Lease ls : leases) {
      try {
        dropLease(ls, null);
      } catch (RuntimeException ignored) {
        // best-effort on close
      }
    }
  }

  private Lock heldLock(String name) {
    synchronized (mu) {
      return held.get(name);
    }
  }

  private Lease heldLease(String name) {
    synchronized (mu) {
      return leased.get(name);
    }
  }

  private Lock adoptLock(LockResponse resp, String name, Duration ttl) {
    Lock lk =
        new Lock(
            name,
            resp.getHolder().isEmpty() ? host.holder() : resp.getHolder(),
            resp.getFencingToken(),
            Grants.fromMs(resp.getDeadlineUnixMs()));
    synchronized (mu) {
      Lock cur = held.get(name);
      if (cur != null && cur.token() == lk.token()) {
        return cur;
      }
      held.put(name, lk);
    }
    startLockRenew(lk, ttl);
    return lk;
  }

  private Lease adoptLease(GrantLeaseResponse resp, String name, Duration ttl) {
    Lease ls =
        new Lease(
            name,
            resp.getOwner().isEmpty() ? host.holder() : resp.getOwner(),
            resp.getFencingToken(),
            Grants.fromMs(resp.getDeadlineUnixMs()));
    synchronized (mu) {
      Lease cur = leased.get(name);
      if (cur != null && cur.token() == ls.token()) {
        return cur;
      }
      leased.put(name, ls);
    }
    startLeaseRenew(ls, ttl);
    return ls;
  }

  private void releaseLock(Lock lk, Duration timeout) {
    lk.stop.set(true);
    cancelRenew("lock:" + lk.name());
    long deadline = host.deadline(timeout);
    try {
      var resp =
          Retry.retry(
              () ->
                  lockStub(deadline)
                      .unlock(
                          UnlockRequest.newBuilder()
                              .setName(lk.name())
                              .setHolder(lk.holder())
                              .setFencingToken(lk.token())
                              .build()),
              deadline,
              Retry::isTransient,
              host::closed);
      if (resp != null && !resp.getReleased()) {
        throw new ClusdrException("clusdr: unlock " + json(lk.name()) + ": " + resp.getMessage());
      }
      forgetLock(lk.name(), lk.token());
    } catch (RuntimeException err) {
      if (Status.fromThrowable(err).getCode() == Status.Code.FAILED_PRECONDITION) {
        forgetLock(lk.name(), lk.token());
      }
      throw ClusdrException.wrap("clusdr: unlock " + json(lk.name()), err);
    }
  }

  private void dropLease(Lease ls, Duration timeout) {
    ls.stop.set(true);
    cancelRenew("lease:" + ls.name());
    long deadline = host.deadline(timeout);
    try {
      var resp =
          Retry.retry(
              () ->
                  leaseStub(deadline)
                      .revoke(
                          RevokeLeaseRequest.newBuilder()
                              .setName(ls.name())
                              .setOwner(ls.owner())
                              .setFencingToken(ls.token())
                              .build()),
              deadline,
              Retry::isTransient,
              host::closed);
      if (resp != null && !resp.getRevoked()) {
        throw new ClusdrException("clusdr: revoke " + json(ls.name()) + ": " + resp.getMessage());
      }
      forgetLease(ls.name(), ls.token());
    } catch (RuntimeException err) {
      if (Status.fromThrowable(err).getCode() == Status.Code.FAILED_PRECONDITION) {
        forgetLease(ls.name(), ls.token());
      }
      throw ClusdrException.wrap("clusdr: revoke " + json(ls.name()), err);
    }
  }

  private void forgetLock(String name, long token) {
    synchronized (mu) {
      Lock cur = held.get(name);
      if (cur != null && cur.token() == token) {
        held.remove(name);
      }
    }
  }

  private void forgetLease(String name, long token) {
    synchronized (mu) {
      Lease cur = leased.get(name);
      if (cur != null && cur.token() == token) {
        leased.remove(name);
      }
    }
  }

  private void startLockRenew(Lock lk, Duration ttl) {
    long interval = Grants.renewIntervalMs(ttl, lk.deadline());
    ScheduledExecutorService exec = host.renewExecutor();
    ScheduledFuture<?> fut =
        exec.scheduleWithFixedDelay(
            () -> {
              if (lk.stop.get() || host.closed()) {
                return;
              }
              try {
                var resp =
                    host.lockBlocking()
                        .withDeadlineAfter(host.requestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .renew(
                            RenewLockRequest.newBuilder()
                                .setName(lk.name())
                                .setHolder(lk.holder())
                                .setFencingToken(lk.token())
                                .setTtlMs(0)
                                .build());
                if (resp.getDeadlineUnixMs() != 0) {
                  lk.setDeadline(Grants.fromMs(resp.getDeadlineUnixMs()));
                }
              } catch (RuntimeException err) {
                Status.Code code = Status.fromThrowable(err).getCode();
                if (code == Status.Code.FAILED_PRECONDITION || code == Status.Code.CANCELLED) {
                  lk.stop.set(true);
                }
              }
            },
            interval,
            interval,
            TimeUnit.MILLISECONDS);
    synchronized (mu) {
      renewals.put("lock:" + lk.name(), fut);
    }
  }

  private void startLeaseRenew(Lease ls, Duration ttl) {
    long interval = Grants.renewIntervalMs(ttl, ls.deadline());
    ScheduledExecutorService exec = host.renewExecutor();
    ScheduledFuture<?> fut =
        exec.scheduleWithFixedDelay(
            () -> {
              if (ls.stop.get() || host.closed()) {
                return;
              }
              try {
                var resp =
                    host.leaseBlocking()
                        .withDeadlineAfter(host.requestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .renew(
                            RenewLeaseRequest.newBuilder()
                                .setName(ls.name())
                                .setOwner(ls.owner())
                                .setFencingToken(ls.token())
                                .setTtlMs(0)
                                .build());
                if (resp.getDeadlineUnixMs() != 0) {
                  ls.setDeadline(Grants.fromMs(resp.getDeadlineUnixMs()));
                }
              } catch (RuntimeException err) {
                Status.Code code = Status.fromThrowable(err).getCode();
                if (code == Status.Code.FAILED_PRECONDITION || code == Status.Code.CANCELLED) {
                  ls.stop.set(true);
                }
              }
            },
            interval,
            interval,
            TimeUnit.MILLISECONDS);
    synchronized (mu) {
      renewals.put("lease:" + ls.name(), fut);
    }
  }

  private void cancelRenew(String key) {
    ScheduledFuture<?> fut;
    synchronized (mu) {
      fut = renewals.remove(key);
    }
    if (fut != null) {
      fut.cancel(false);
    }
  }

  private LockServiceGrpc.LockServiceBlockingStub lockStub(long deadlineMs) {
    return host.lockBlocking().withDeadlineAfter(Retry.remainingMs(deadlineMs), TimeUnit.MILLISECONDS);
  }

  private LeaseServiceGrpc.LeaseServiceBlockingStub leaseStub(long deadlineMs) {
    return host.leaseBlocking().withDeadlineAfter(Retry.remainingMs(deadlineMs), TimeUnit.MILLISECONDS);
  }

  private static String json(String s) {
    return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }
}
