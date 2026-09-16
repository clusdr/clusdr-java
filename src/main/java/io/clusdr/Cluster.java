package io.clusdr;

import com.google.protobuf.ByteString;
import io.clusdr.v1alpha1.EventServiceGrpc;
import io.clusdr.v1alpha1.GetLeaderRequest;
import io.clusdr.v1alpha1.HealthRequest;
import io.clusdr.v1alpha1.HealthServiceGrpc;
import io.clusdr.v1alpha1.LeaseServiceGrpc;
import io.clusdr.v1alpha1.ListMembersRequest;
import io.clusdr.v1alpha1.LockServiceGrpc;
import io.clusdr.v1alpha1.MembershipServiceGrpc;
import io.clusdr.v1alpha1.PublishEventRequest;
import io.clusdr.v1alpha1.WatchServiceGrpc;
import io.grpc.ManagedChannel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Connection to the local daemon. Thread-safe for unary calls. Same connection
 * is the same holder ({@code unlock} is process-wide for that name). Several
 * {@link #watch()} streams on one client are fine.
 */
public final class Cluster implements AutoCloseable {
  private final Options opts;
  private final String addr;
  private final String holder;
  private final ManagedChannel channel;
  private final MembershipServiceGrpc.MembershipServiceBlockingStub mem;
  private final WatchServiceGrpc.WatchServiceStub watch;
  private final EventServiceGrpc.EventServiceBlockingStub ev;
  private final HealthServiceGrpc.HealthServiceBlockingStub health;
  private final LockServiceGrpc.LockServiceBlockingStub lock;
  private final LeaseServiceGrpc.LeaseServiceBlockingStub lease;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final Coord coord;
  private final Set<Watch> watches = ConcurrentHashMap.newKeySet();
  private final ScheduledExecutorService renew =
      Executors.newSingleThreadScheduledExecutor(
          r -> {
            Thread t = new Thread(r, "clusdr-renew");
            t.setDaemon(true);
            return t;
          });

  Cluster(Options opts, String addr, ManagedChannel channel, String holder) {
    this.opts = opts;
    this.addr = addr;
    this.holder = holder;
    this.channel = channel;
    this.mem = MembershipServiceGrpc.newBlockingStub(channel);
    this.watch = WatchServiceGrpc.newStub(channel);
    this.ev = EventServiceGrpc.newBlockingStub(channel);
    this.health = HealthServiceGrpc.newBlockingStub(channel);
    this.lock = LockServiceGrpc.newBlockingStub(channel);
    this.lease = LeaseServiceGrpc.newBlockingStub(channel);
    this.coord = new Coord(this);
  }

  public String holder() {
    return holder;
  }

  public boolean closed() {
    return closed.get();
  }

  Duration requestTimeout() {
    return opts.requestTimeout;
  }

  LockServiceGrpc.LockServiceBlockingStub lockBlocking() {
    return lock;
  }

  LeaseServiceGrpc.LeaseServiceBlockingStub leaseBlocking() {
    return lease;
  }

  WatchServiceGrpc.WatchServiceStub watchStub() {
    return watch;
  }

  ScheduledExecutorService renewExecutor() {
    return renew;
  }

  public List<Member> members() {
    return members(null);
  }

  public List<Member> members(Duration timeout) {
    long deadline = deadline(timeout);
    try {
      var resp =
          Retry.retry(
              () ->
                  mem.withDeadlineAfter(Retry.remainingMs(deadline), TimeUnit.MILLISECONDS)
                      .listMembers(ListMembersRequest.getDefaultInstance()),
              deadline,
              Retry::isTransient,
              this::closed);
      List<Member> out = new ArrayList<>();
      for (var m : resp.getMembersList()) {
        out.add(new Member(m.getId(), m.getAddress(), m.getStatus(), m.getLeader(), m.getRole()));
      }
      return List.copyOf(out);
    } catch (RuntimeException err) {
      throw ClusdrException.wrap("clusdr: members", err);
    }
  }

  public Member leader() {
    return leader(null);
  }

  public Member leader(Duration timeout) {
    long deadline = deadline(timeout);
    try {
      var resp =
          Retry.retry(
              () ->
                  mem.withDeadlineAfter(Retry.remainingMs(deadline), TimeUnit.MILLISECONDS)
                      .getLeader(GetLeaderRequest.getDefaultInstance()),
              deadline,
              Retry::isTransient,
              this::closed);
      if (resp.getLeaderId().isEmpty()) {
        throw new ClusdrException("clusdr: leader: UNAVAILABLE: no leader");
      }
      return new Member(resp.getLeaderId(), resp.getAddress(), "alive", true, "voter");
    } catch (RuntimeException err) {
      throw ClusdrException.wrap("clusdr: leader", err);
    }
  }

  public void publish(String topic) {
    publish(topic, null, null);
  }

  public void publish(String topic, Object payload) {
    publish(topic, payload, null);
  }

  public void publish(String topic, Object payload, Duration timeout) {
    byte[] body = Payloads.encode(payload);
    if (body.length > Options.MAX_PAYLOAD) {
      throw new ClusdrException("clusdr: publish payload exceeds " + Options.MAX_PAYLOAD + " bytes");
    }
    long deadline = deadline(timeout);
    try {
      var resp =
          Retry.retry(
              () ->
                  ev.withDeadlineAfter(Retry.remainingMs(deadline), TimeUnit.MILLISECONDS)
                      .publishEvent(
                          PublishEventRequest.newBuilder()
                              .setTopic(topic)
                              .setPayload(ByteString.copyFrom(body))
                              .build()),
              deadline,
              Retry::isTransient,
              this::closed);
      if (resp != null && !resp.getAccepted()) {
        throw new ClusdrException("clusdr: publish rejected: " + resp.getMessage());
      }
    } catch (RuntimeException err) {
      throw ClusdrException.wrap("clusdr: publish", err);
    }
  }

  /**
   * Stream events. Empty filter is the full bus.
   *
   * <p>Reconnects with {@code lastSeq} on drop. Closing the stream or {@link #close()}
   * ends it. Several Watch streams on one client are fine.
   */
  public Watch watch() {
    return watch(WatchFilter.all());
  }

  public Watch watch(WatchFilter filter) {
    WatchFilter normalized = WatchFilter.normalize(filter);
    Watch w = new Watch(this, normalized);
    watches.add(w);
    return w;
  }

  public Lock lock(String name) {
    return lock(name, null, null);
  }

  public Lock lock(String name, Duration ttl) {
    return lock(name, ttl, null);
  }

  public Lock lock(String name, Duration ttl, Duration timeout) {
    return coord.lock(name, ttl, timeout);
  }

  public Optional<Lock> tryLock(String name) {
    return tryLock(name, null, null);
  }

  public Optional<Lock> tryLock(String name, Duration ttl) {
    return tryLock(name, ttl, null);
  }

  public Optional<Lock> tryLock(String name, Duration ttl, Duration timeout) {
    return coord.tryLock(name, ttl, timeout);
  }

  public void unlock(String name) {
    unlock(name, null);
  }

  public void unlock(String name, Duration timeout) {
    coord.unlock(name, timeout);
  }

  public Lease lease(String name) {
    return lease(name, null, null);
  }

  public Lease lease(String name, Duration ttl) {
    return lease(name, ttl, null);
  }

  public Lease lease(String name, Duration ttl, Duration timeout) {
    return coord.lease(name, ttl, timeout);
  }

  public void renew(String name) {
    renew(name, null);
  }

  public void renew(String name, Duration timeout) {
    coord.renew(name, timeout);
  }

  public void revoke(String name) {
    revoke(name, null);
  }

  public void revoke(String name, Duration timeout) {
    coord.revoke(name, timeout);
  }

  @Override
  public void close() {
    if (closed.get()) {
      shutdownChannel();
      return;
    }
    coord.releaseGrants();
    closed.set(true);
    for (Watch w : watches) {
      w.close();
    }
    watches.clear();
    renew.shutdownNow();
    shutdownChannel();
  }

  void dropWatch(Watch w) {
    watches.remove(w);
  }

  long deadline(Duration timeout) {
    Duration d = timeout == null ? opts.requestTimeout : timeout;
    return System.currentTimeMillis() + d.toMillis();
  }

  void waitReady() {
    if (opts.readyTimeout.isZero()) {
      return;
    }
    long deadline = System.currentTimeMillis() + opts.readyTimeout.toMillis();
    try {
      Retry.retry(
          () -> {
            long slice = Math.min(500, Math.max(50, deadline - System.currentTimeMillis()));
            return health
                .withDeadlineAfter(slice, TimeUnit.MILLISECONDS)
                .health(HealthRequest.getDefaultInstance());
          },
          deadline,
          Retry::readyTransient,
          this::closed);
    } catch (RuntimeException err) {
      close();
      throw new ClusdrException("clusdr: daemon not ready at " + addr + ": " + err.getMessage(), err);
    }
  }

  private void shutdownChannel() {
    channel.shutdownNow();
    try {
      channel.awaitTermination(2, TimeUnit.SECONDS);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }
}
