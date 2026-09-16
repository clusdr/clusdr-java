package io.clusdr;

import io.clusdr.v1alpha1.WatchRequest;
import io.clusdr.v1alpha1.WatchResponse;
import io.clusdr.v1alpha1.WatchServiceGrpc;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.time.Instant;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Blocking Watch stream. Reconnects with {@code lastSeq} on drop. Closing the
 * stream or the {@link Cluster} ends it.
 */
public final class Watch implements AutoCloseable, Iterable<Event>, Iterator<Event> {
  private final Cluster cluster;
  private final WatchFilter filter;
  private final BlockingQueue<Item> queue = new LinkedBlockingQueue<>();
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicLong lastSeq = new AtomicLong(0);
  private final AtomicReference<Context.CancellableContext> call = new AtomicReference<>();
  private final Thread loop;
  private Item peeked;

  Watch(Cluster cluster, WatchFilter filter) {
    this.cluster = cluster;
    this.filter = filter;
    this.loop = new Thread(this::run, "clusdr-watch");
    this.loop.setDaemon(true);
    this.loop.start();
  }

  @Override
  public Iterator<Event> iterator() {
    return this;
  }

  @Override
  public boolean hasNext() {
    if (peeked != null) {
      return peeked.event != null && peeked.error == null;
    }
    peeked = take();
    if (peeked == null) {
      return false;
    }
    if (peeked.error != null) {
      throw ClusdrException.wrap("clusdr: watch", peeked.error);
    }
    return peeked.event != null;
  }

  @Override
  public Event next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    Event ev = peeked.event;
    peeked = null;
    return ev;
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    cancelCall();
    loop.interrupt();
    queue.offer(Item.end());
    cluster.dropWatch(this);
  }

  private void run() {
    long backoff = Options.INITIAL_BACKOFF_MS;
    while (!closed.get() && !cluster.closed()) {
      Context.CancellableContext ctx = Context.current().withCancellation();
      call.set(ctx);
      Context prev = ctx.attach();
      try {
        WatchRequest req =
            WatchRequest.newBuilder()
                .setLastSeq(lastSeq.get())
                .addAllTopics(filter.topics())
                .addAllEventTypes(filter.eventTypes())
                .build();
        StreamDone done = new StreamDone();
        cluster.watchStub().watch(req, new Observer(done));
        done.await();
        if (done.invalid) {
          queue.offer(Item.error(done.error));
          return;
        }
        backoff = Options.INITIAL_BACKOFF_MS;
      } catch (Exception err) {
        if (closed.get() || cluster.closed() || Retry.cancelled(err)) {
          return;
        }
        if (Status.fromThrowable(err).getCode() == Status.Code.INVALID_ARGUMENT) {
          queue.offer(Item.error(err));
          return;
        }
      } finally {
        ctx.detach(prev);
        ctx.cancel(null);
        call.compareAndSet(ctx, null);
      }
      if (closed.get() || cluster.closed()) {
        return;
      }
      try {
        Thread.sleep(backoff);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        return;
      }
      backoff = Math.min(backoff * 2, Options.MAX_BACKOFF_MS);
    }
    queue.offer(Item.end());
  }

  private void cancelCall() {
    Context.CancellableContext ctx = call.getAndSet(null);
    if (ctx != null) {
      ctx.cancel(null);
    }
  }

  private Item take() {
    while (!closed.get() || !queue.isEmpty()) {
      try {
        Item item = queue.poll(50, TimeUnit.MILLISECONDS);
        if (item != null) {
          return item.end ? Item.end() : item;
        }
        if (closed.get() && queue.isEmpty()) {
          return Item.end();
        }
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        return Item.end();
      }
    }
    return Item.end();
  }

  private final class Observer implements StreamObserver<WatchResponse> {
    private final StreamDone done;

    Observer(StreamDone done) {
      this.done = done;
    }

    @Override
    public void onNext(WatchResponse resp) {
      if (resp.getSeq() > lastSeq.get()) {
        lastSeq.set(resp.getSeq());
      }
      queue.offer(Item.event(toEvent(resp)));
    }

    @Override
    public void onError(Throwable t) {
      if (Status.fromThrowable(t).getCode() == Status.Code.INVALID_ARGUMENT) {
        done.failInvalid(t);
        return;
      }
      done.complete();
    }

    @Override
    public void onCompleted() {
      done.complete();
    }
  }

  private static Event toEvent(WatchResponse resp) {
    Instant ts = Grants.fromMs(resp.getTimestampUnixMs());
    return new Event(
        resp.getType(),
        resp.getSource(),
        resp.getPayload().toByteArray(),
        ts,
        resp.getSeq());
  }

  private static final class StreamDone {
    private boolean finished;
    volatile boolean invalid;
    volatile Throwable error;

    synchronized void complete() {
      finished = true;
      notifyAll();
    }

    synchronized void failInvalid(Throwable t) {
      invalid = true;
      error = t;
      finished = true;
      notifyAll();
    }

    synchronized void await() throws InterruptedException {
      while (!finished) {
        wait();
      }
    }
  }

  private static final class Item {
    final Event event;
    final Throwable error;
    final boolean end;

    private Item(Event event, Throwable error, boolean end) {
      this.event = event;
      this.error = error;
      this.end = end;
    }

    static Item event(Event ev) {
      return new Item(ev, null, false);
    }

    static Item error(Throwable t) {
      return new Item(null, t, false);
    }

    static Item end() {
      return new Item(null, null, true);
    }
  }
}
