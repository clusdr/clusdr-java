package io.clusdr;

import io.grpc.ManagedChannel;

/** Entry points for the local-daemon application SDK. */
public final class Clusdr {
  private Clusdr() {}

  /** Connect to the daemon on this host. */
  public static Cluster local() {
    return local(Options.defaults());
  }

  public static Cluster local(Options opts) {
    return connect(Options.envAddr(), opts);
  }

  /**
   * Connect to {@code addr} (Runtime API host:port).
   *
   * <p>Tests and a second daemon on this host. Applications use {@link #local()}.
   */
  public static Cluster dial(String addr) {
    return dial(addr, Options.defaults());
  }

  public static Cluster dial(String addr, Options opts) {
    if (addr == null || addr.isBlank()) {
      throw new ClusdrException("clusdr: empty dial address");
    }
    return connect(addr.trim(), opts);
  }

  private static Cluster connect(String addr, Options opts) {
    if (opts == null) {
      opts = Options.defaults();
    }
    if (addr == null || addr.isBlank()) {
      throw new ClusdrException("clusdr: empty dial address");
    }
    ManagedChannel channel = Tls.channel(addr, opts);
    String holder = opts.holder.isEmpty() ? Grants.newHolder() : opts.holder;
    Cluster cluster = new Cluster(opts, addr, channel, holder);
    cluster.waitReady();
    return cluster;
  }
}
