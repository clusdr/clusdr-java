package io.clusdr;

import com.google.protobuf.ByteString;
import io.clusdr.v1alpha1.EventServiceGrpc;
import io.clusdr.v1alpha1.GrantRequest;
import io.clusdr.v1alpha1.GrantResponse;
import io.clusdr.v1alpha1.HealthRequest;
import io.clusdr.v1alpha1.HealthResponse;
import io.clusdr.v1alpha1.HealthServiceGrpc;
import io.clusdr.v1alpha1.LeaseServiceGrpc;
import io.clusdr.v1alpha1.ListMembersRequest;
import io.clusdr.v1alpha1.ListMembersResponse;
import io.clusdr.v1alpha1.LockRequest;
import io.clusdr.v1alpha1.LockResponse;
import io.clusdr.v1alpha1.LockServiceGrpc;
import io.clusdr.v1alpha1.TryLockRequest;
import io.clusdr.v1alpha1.TryLockResponse;
import io.clusdr.v1alpha1.Member;
import io.clusdr.v1alpha1.MembershipServiceGrpc;
import io.clusdr.v1alpha1.PublishEventRequest;
import io.clusdr.v1alpha1.PublishEventResponse;
import io.clusdr.v1alpha1.LeaseServiceRenewRequest;
import io.clusdr.v1alpha1.LeaseServiceRenewResponse;
import io.clusdr.v1alpha1.LockServiceRenewRequest;
import io.clusdr.v1alpha1.LockServiceRenewResponse;
import io.clusdr.v1alpha1.RevokeRequest;
import io.clusdr.v1alpha1.RevokeResponse;
import io.clusdr.v1alpha1.UnlockRequest;
import io.clusdr.v1alpha1.UnlockResponse;
import io.clusdr.v1alpha1.WatchRequest;
import io.clusdr.v1alpha1.WatchResponse;
import io.clusdr.v1alpha1.WatchServiceGrpc;
import io.grpc.Context;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

final class FakeDaemon {
  final Server server;
  final String addr;
  final FakeState state;

  FakeDaemon(HealthServiceGrpc.HealthServiceImplBase health) {
    this.state = new FakeState();
    try {
      this.server =
          Grpc.newServerBuilderForPort(0, InsecureServerCredentials.create())
              .addService(health)
              .addService(new MembershipImpl(state))
              .addService(new EventsImpl(state))
              .addService(new WatchImpl(state))
              .addService(new LocksImpl(state.coord))
              .addService(new LeasesImpl(state.coord))
              .build()
              .start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.addr = "127.0.0.1:" + server.getPort();
  }

  static FakeDaemon start() {
    return new FakeDaemon(new OkHealth());
  }

  void stop() {
    server.shutdownNow();
    try {
      server.awaitTermination(2, TimeUnit.SECONDS);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }

  static final class Grant {
    final String name;
    String holder;
    long token;
    long deadline;
    double ttl;

    Grant(String name, String holder, long token, long deadline, double ttl) {
      this.name = name;
      this.holder = holder;
      this.token = token;
      this.deadline = deadline;
      this.ttl = ttl;
    }
  }

  static final class CoordTable {
    final Map<String, Grant> locks = new LinkedHashMap<>();
    final Map<String, Grant> leases = new LinkedHashMap<>();
    long nextLock;
    long nextLease;

    synchronized Grant[] acquireLock(String name, String holder, double ttl) {
      expireLocked(locks);
      Grant rec = locks.get(name);
      if (rec != null) {
        if (rec.holder.equals(holder)) {
          rec.deadline = System.currentTimeMillis() + (long) (ttl * 1000);
          rec.ttl = ttl;
          notifyAll();
          return new Grant[] {rec, granted()};
        }
        return new Grant[] {rec, denied()};
      }
      nextLock += 1;
      Grant created =
          new Grant(name, holder, nextLock, System.currentTimeMillis() + (long) (ttl * 1000), ttl);
      locks.put(name, created);
      notifyAll();
      return new Grant[] {created, granted()};
    }

    synchronized void releaseLock(String name, String holder, long token) {
      Grant rec = locks.get(name);
      if (rec == null || !rec.holder.equals(holder) || rec.token != token) {
        throw new IllegalStateException("fencing token mismatch");
      }
      locks.remove(name);
      notifyAll();
    }

    synchronized Grant renewLock(String name, String holder, long token, double ttl) {
      Grant rec = locks.get(name);
      if (rec == null || !rec.holder.equals(holder) || rec.token != token) {
        throw new IllegalStateException("fencing token mismatch");
      }
      rec.deadline = System.currentTimeMillis() + (long) (ttl * 1000);
      rec.ttl = ttl;
      return rec;
    }

    synchronized Grant[] grantLease(String name, String owner, double ttl) {
      expireLocked(leases);
      Grant rec = leases.get(name);
      if (rec != null) {
        if (rec.holder.equals(owner)) {
          rec.deadline = System.currentTimeMillis() + (long) (ttl * 1000);
          rec.ttl = ttl;
          return new Grant[] {rec, granted()};
        }
        return new Grant[] {rec, denied()};
      }
      nextLease += 1;
      Grant created =
          new Grant(name, owner, nextLease, System.currentTimeMillis() + (long) (ttl * 1000), ttl);
      leases.put(name, created);
      return new Grant[] {created, granted()};
    }

    synchronized void revokeLease(String name, String owner, long token) {
      Grant rec = leases.get(name);
      if (rec == null || !rec.holder.equals(owner) || rec.token != token) {
        throw new IllegalStateException("fencing token mismatch");
      }
      leases.remove(name);
    }

    synchronized Grant renewLease(String name, String owner, long token, double ttl) {
      Grant rec = leases.get(name);
      if (rec == null || !rec.holder.equals(owner) || rec.token != token) {
        throw new IllegalStateException("fencing token mismatch");
      }
      rec.deadline = System.currentTimeMillis() + (long) (ttl * 1000);
      rec.ttl = ttl;
      return rec;
    }

    synchronized void expireDue() {
      expireLocked(locks);
      expireLocked(leases);
      notifyAll();
    }

    synchronized void waitFor(long ms) throws InterruptedException {
      wait(ms);
    }

    private void expireLocked(Map<String, Grant> table) {
      long now = System.currentTimeMillis();
      Iterator<Map.Entry<String, Grant>> it = table.entrySet().iterator();
      while (it.hasNext()) {
        if (it.next().getValue().deadline <= now) {
          it.remove();
        }
      }
    }

    private static Grant granted() {
      return new Grant("", "", 1, 0, 0);
    }

    private static Grant denied() {
      return new Grant("", "", 0, 0, 0);
    }

    static boolean ok(Grant[] pair) {
      return pair[1].token == 1;
    }
  }

  static final class FakeState {
    final List<Member> members =
        List.of(
            Member.newBuilder()
                .setId("node-a")
                .setAddress("127.0.0.1:1")
                .setStatus("alive")
                .setLeader(true)
                .setRole("")
                .build());
    final List<WatchResponse> events = new ArrayList<>();
    final List<PublishEventRequest> published = new CopyOnWriteArrayList<>();
    final CoordTable coord = new CoordTable();
    private final CopyOnWriteArrayList<BlockingQueue<WatchResponse>> subscribers =
        new CopyOnWriteArrayList<>();

    BlockingQueue<WatchResponse> subscribe() {
      LinkedBlockingQueue<WatchResponse> q = new LinkedBlockingQueue<>();
      subscribers.add(q);
      return q;
    }

    void unsubscribe(BlockingQueue<WatchResponse> q) {
      subscribers.remove(q);
    }

    void pushEvent(WatchResponse ev) {
      events.add(ev);
      for (BlockingQueue<WatchResponse> q : subscribers) {
        q.offer(ev);
      }
    }
  }

  static double ttlS(long ttlMs, double reuse) {
    if (ttlMs > 0) {
      return ttlMs / 1000.0;
    }
    return reuse;
  }

  static boolean watchMatch(String eventType, List<String> topics, List<String> types) {
    if (eventType.equals("watch.sync") || eventType.equals("watch.gap")) {
      return true;
    }
    if (!topics.isEmpty()) {
      if (!eventType.startsWith("custom.") || !topics.contains(eventType.substring(7))) {
        return false;
      }
    }
    if (!types.isEmpty() && !types.contains(eventType)) {
      return false;
    }
    return true;
  }

  static final class OkHealth extends HealthServiceGrpc.HealthServiceImplBase {
    @Override
    public void health(HealthRequest request, StreamObserver<HealthResponse> responseObserver) {
      responseObserver.onNext(
          HealthResponse.newBuilder()
              .setNodeId("node-a")
              .setClusterId("c1")
              .setRole("leader")
              .setHealthy(true)
              .build());
      responseObserver.onCompleted();
    }
  }

  static final class FlakyHealth extends HealthServiceGrpc.HealthServiceImplBase {
    private final int failures;
    final AtomicInteger calls = new AtomicInteger();

    FlakyHealth(int failures) {
      this.failures = failures;
    }

    @Override
    public void health(HealthRequest request, StreamObserver<HealthResponse> responseObserver) {
      int n = calls.incrementAndGet();
      if (n <= failures) {
        responseObserver.onError(Status.UNAVAILABLE.withDescription("wait").asRuntimeException());
        return;
      }
      responseObserver.onNext(
          HealthResponse.newBuilder()
              .setNodeId("node-a")
              .setClusterId("c1")
              .setRole("leader")
              .setHealthy(true)
              .build());
      responseObserver.onCompleted();
    }
  }

  static final class MembershipImpl extends MembershipServiceGrpc.MembershipServiceImplBase {
    private final FakeState state;

    MembershipImpl(FakeState state) {
      this.state = state;
    }

    @Override
    public void listMembers(ListMembersRequest request, StreamObserver<ListMembersResponse> obs) {
      obs.onNext(ListMembersResponse.newBuilder().addAllMembers(state.members).build());
      obs.onCompleted();
    }

    @Override
    public void getLeader(
        io.clusdr.v1alpha1.GetLeaderRequest request,
        StreamObserver<io.clusdr.v1alpha1.GetLeaderResponse> obs) {
      for (Member m : state.members) {
        if (m.getLeader()) {
          obs.onNext(
              io.clusdr.v1alpha1.GetLeaderResponse.newBuilder()
                  .setLeaderId(m.getId())
                  .setAddress(m.getAddress())
                  .build());
          obs.onCompleted();
          return;
        }
      }
      obs.onNext(io.clusdr.v1alpha1.GetLeaderResponse.getDefaultInstance());
      obs.onCompleted();
    }
  }

  static final class EventsImpl extends EventServiceGrpc.EventServiceImplBase {
    private final FakeState state;

    EventsImpl(FakeState state) {
      this.state = state;
    }

    @Override
    public void publishEvent(PublishEventRequest request, StreamObserver<PublishEventResponse> obs) {
      state.published.add(request);
      WatchResponse ev =
          WatchResponse.newBuilder()
              .setType("custom." + request.getTopic())
              .setSource("node-a")
              .setPayload(request.getPayload())
              .setTimestampUnixMs(1)
              .setSeq(state.published.size())
              .build();
      state.pushEvent(ev);
      obs.onNext(
          PublishEventResponse.newBuilder().setAccepted(true).setType(ev.getType()).build());
      obs.onCompleted();
    }
  }

  static final class WatchImpl extends WatchServiceGrpc.WatchServiceImplBase {
    private final FakeState state;

    WatchImpl(FakeState state) {
      this.state = state;
    }

    @Override
    public void watch(WatchRequest request, StreamObserver<WatchResponse> obs) {
      List<String> topics = new ArrayList<>();
      for (String t : request.getTopicsList()) {
        topics.add(t.startsWith("custom.") ? t.substring(7) : t);
      }
      List<String> types = new ArrayList<>(request.getEventTypesList());
      AtomicBoolean stopped = new AtomicBoolean(false);
      if (obs instanceof ServerCallStreamObserver<?> so) {
        so.setOnCancelHandler(() -> stopped.set(true));
      }
      Thread t =
          new Thread(
              () -> {
                BlockingQueue<WatchResponse> q = state.subscribe();
                try {
                  WatchResponse snap =
                      WatchResponse.newBuilder()
                          .setType("member.join")
                          .setSource("node-a")
                          .setPayload(ByteString.EMPTY)
                          .setTimestampUnixMs(1)
                          .setSeq(0)
                          .build();
                  if (watchMatch(snap.getType(), topics, types) && !stopped.get()) {
                    obs.onNext(snap);
                  }
                  while (!stopped.get()) {
                    WatchResponse ev = q.poll(50, TimeUnit.MILLISECONDS);
                    if (stopped.get()) {
                      return;
                    }
                    if (ev == null) {
                      continue;
                    }
                    if (!watchMatch(ev.getType(), topics, types)) {
                      continue;
                    }
                    obs.onNext(ev);
                  }
                } catch (InterruptedException ie) {
                  Thread.currentThread().interrupt();
                } catch (RuntimeException ignored) {
                  // stream cancelled
                } finally {
                  state.unsubscribe(q);
                }
              },
              "fake-watch");
      t.setDaemon(true);
      t.start();
    }
  }

  static final class LocksImpl extends LockServiceGrpc.LockServiceImplBase {
    private final CoordTable table;

    LocksImpl(CoordTable table) {
      this.table = table;
    }

    @Override
    public void lock(LockRequest request, StreamObserver<LockResponse> obs) {
      double ttl = ttlS(request.getTtlMs(), 15);
      while (!Context.current().isCancelled()) {
        Grant[] pair = table.acquireLock(request.getName(), request.getHolder(), ttl);
        if (CoordTable.ok(pair)) {
          Grant rec = pair[0];
          obs.onNext(
              LockResponse.newBuilder()
                  .setAcquired(true)
                  .setFencingToken(rec.token)
                  .setHolder(rec.holder)
                  .setDeadlineUnixMs(rec.deadline)
                  .build());
          obs.onCompleted();
          return;
        }
        try {
          table.waitFor(50);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          obs.onError(Status.CANCELLED.asRuntimeException());
          return;
        }
      }
      obs.onError(Status.CANCELLED.asRuntimeException());
    }

    @Override
    public void tryLock(TryLockRequest request, StreamObserver<TryLockResponse> obs) {
      Grant[] pair = table.acquireLock(request.getName(), request.getHolder(), ttlS(request.getTtlMs(), 15));
      Grant rec = pair[0];
      if (!CoordTable.ok(pair)) {
        obs.onNext(
            TryLockResponse.newBuilder()
                .setAcquired(false)
                .setMessage("held")
                .setFencingToken(rec.token)
                .setHolder(rec.holder)
                .setDeadlineUnixMs(rec.deadline)
                .build());
        obs.onCompleted();
        return;
      }
      obs.onNext(
          TryLockResponse.newBuilder()
              .setAcquired(true)
              .setFencingToken(rec.token)
              .setHolder(rec.holder)
              .setDeadlineUnixMs(rec.deadline)
              .build());
      obs.onCompleted();
    }

    @Override
    public void unlock(UnlockRequest request, StreamObserver<UnlockResponse> obs) {
      try {
        table.releaseLock(request.getName(), request.getHolder(), request.getFencingToken());
      } catch (IllegalStateException err) {
        obs.onError(Status.FAILED_PRECONDITION.withDescription(err.getMessage()).asRuntimeException());
        return;
      }
      obs.onNext(UnlockResponse.newBuilder().setReleased(true).build());
      obs.onCompleted();
    }

    @Override
    public void renew(LockServiceRenewRequest request, StreamObserver<LockServiceRenewResponse> obs) {
      try {
        Grant cur;
        synchronized (table) {
          cur = table.locks.get(request.getName());
        }
        double reuse = cur != null ? cur.ttl : 15;
        Grant rec =
            table.renewLock(
                request.getName(),
                request.getHolder(),
                request.getFencingToken(),
                ttlS(request.getTtlMs(), reuse));
        obs.onNext(
            LockServiceRenewResponse.newBuilder()
                .setRenewed(true)
                .setFencingToken(request.getFencingToken())
                .setDeadlineUnixMs(rec.deadline)
                .build());
        obs.onCompleted();
      } catch (IllegalStateException err) {
        obs.onError(Status.FAILED_PRECONDITION.withDescription(err.getMessage()).asRuntimeException());
      }
    }
  }

  static final class LeasesImpl extends LeaseServiceGrpc.LeaseServiceImplBase {
    private final CoordTable table;

    LeasesImpl(CoordTable table) {
      this.table = table;
    }

    @Override
    public void grant(GrantRequest request, StreamObserver<GrantResponse> obs) {
      Grant[] pair = table.grantLease(request.getName(), request.getOwner(), ttlS(request.getTtlMs(), 15));
      Grant rec = pair[0];
      if (!CoordTable.ok(pair)) {
        obs.onNext(
            GrantResponse.newBuilder()
                .setGranted(false)
                .setMessage("held")
                .setFencingToken(rec.token)
                .setOwner(rec.holder)
                .setDeadlineUnixMs(rec.deadline)
                .build());
        obs.onCompleted();
        return;
      }
      obs.onNext(
          GrantResponse.newBuilder()
              .setGranted(true)
              .setFencingToken(rec.token)
              .setOwner(rec.holder)
              .setDeadlineUnixMs(rec.deadline)
              .build());
      obs.onCompleted();
    }

    @Override
    public void renew(LeaseServiceRenewRequest request, StreamObserver<LeaseServiceRenewResponse> obs) {
      try {
        Grant cur;
        synchronized (table) {
          cur = table.leases.get(request.getName());
        }
        double reuse = cur != null ? cur.ttl : 15;
        Grant rec =
            table.renewLease(
                request.getName(),
                request.getOwner(),
                request.getFencingToken(),
                ttlS(request.getTtlMs(), reuse));
        obs.onNext(
            LeaseServiceRenewResponse.newBuilder()
                .setRenewed(true)
                .setFencingToken(request.getFencingToken())
                .setDeadlineUnixMs(rec.deadline)
                .build());
        obs.onCompleted();
      } catch (IllegalStateException err) {
        obs.onError(Status.FAILED_PRECONDITION.withDescription(err.getMessage()).asRuntimeException());
      }
    }

    @Override
    public void revoke(RevokeRequest request, StreamObserver<RevokeResponse> obs) {
      try {
        table.revokeLease(request.getName(), request.getOwner(), request.getFencingToken());
      } catch (IllegalStateException err) {
        obs.onError(Status.FAILED_PRECONDITION.withDescription(err.getMessage()).asRuntimeException());
        return;
      }
      obs.onNext(RevokeResponse.newBuilder().setRevoked(true).build());
      obs.onCompleted();
    }
  }
}
