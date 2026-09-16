package io.clusdr;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Named TTL grant held by this client. */
public final class Lease {
  private final String name;
  private final String owner;
  private final long token;
  private final AtomicReference<Instant> deadline;
  final AtomicBoolean stop = new AtomicBoolean(false);

  Lease(String name, String owner, long token, Instant deadline) {
    this.name = name;
    this.owner = owner;
    this.token = token;
    this.deadline = new AtomicReference<>(deadline);
  }

  public String name() {
    return name;
  }

  public String owner() {
    return owner;
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

  /** Stop background renewal; the grant then expires at its deadline. */
  public void stopRenew() {
    stop.set(true);
  }
}
