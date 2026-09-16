package io.clusdr;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

final class Grants {
  private Grants() {}

  static Instant fromMs(long unixMs) {
    if (unixMs <= 0) {
      return null;
    }
    return Instant.ofEpochMilli(unixMs);
  }

  static long ttlMs(Duration ttl) {
    if (ttl == null || ttl.isZero() || ttl.isNegative()) {
      return 0;
    }
    return Math.max(1, ttl.toMillis());
  }

  static long renewIntervalMs(Duration ttl, Instant deadline) {
    Duration d = ttl;
    if (d == null || d.isZero() || d.isNegative()) {
      if (deadline != null) {
        d = Duration.between(Instant.now(), deadline);
      }
    }
    if (d == null || d.isZero() || d.isNegative()) {
      d = Duration.ofSeconds(15);
    }
    return Math.max(50, d.toMillis() / 3);
  }

  static String newHolder() {
    return "sdk-" + UUID.randomUUID();
  }
}
