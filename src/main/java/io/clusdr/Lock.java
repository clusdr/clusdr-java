package io.clusdr;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Exclusive lock grant held by this client. */
public final class Lock {
  private final String name;
  private final String holder;
  private final long token;
  private final AtomicReference<Instant> deadline;
  final AtomicBoolean stop = new AtomicBoolean(false);

  Lock(String name, String holder, long token, Instant deadline) {
    this.name = name;
    this.holder = holder;
    this.token = token;
    this.deadline = new AtomicReference<>(deadline);
  }

  public String name() {
    return name;
  }

  public String holder() {
    return holder;
  }

  public long token() {
    return token;
  }

  public Instant deadline() {
    return deadline.get();
  }

  void setDeadline(Instant value) {
    deadline.set(value);
  }
}
