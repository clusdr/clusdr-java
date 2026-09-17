package io.clusdr;

import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

/** A Watch event. Crash is {@code member.dead}; leave is {@code member.left}. */
public final class Event {
  private final String type;
  private final String source;
  private final byte[] payload;
  private final Instant timestamp;
  private final long seq;

  public Event(String type, String source, byte[] payload, Instant timestamp, long seq) {
    this.type = type == null ? "" : type;
    this.source = source == null ? "" : source;
    this.payload = payload == null ? new byte[0] : payload;
    this.timestamp = timestamp == null ? Instant.EPOCH : timestamp;
    this.seq = seq;
  }

  /** Type string: {@code member.join}, {@code member.dead}, {@code member.left}, {@code leader.changed}, {@code custom.<topic>}, … */
  public String type() {
    return type;
  }

  public String source() {
    return source;
  }

  /** Opaque payload bytes. The array is not copied; treat it as read-only. */
  public byte[] payload() {
    return payload;
  }

  public Instant timestamp() {
    return timestamp;
  }

  public long seq() {
    return seq;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Event e)) {
      return false;
    }
    return seq == e.seq
        && type.equals(e.type)
        && source.equals(e.source)
        && timestamp.equals(e.timestamp)
        && Arrays.equals(payload, e.payload);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, source, timestamp, seq, Arrays.hashCode(payload));
  }

  @Override
  public String toString() {
    return "Event{type=" + type + ", source=" + source + ", seq=" + seq + "}";
  }
}
