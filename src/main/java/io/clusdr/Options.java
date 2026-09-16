package io.clusdr;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;

/** Connection options for {@link Clusdr#local(Options)} and {@link Clusdr#dial(String, Options)}. */
public final class Options {
  static final String DEFAULT_ADDR = "127.0.0.1:7947";
  static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(10);
  static final Duration DEFAULT_READY_TIMEOUT = Duration.ofSeconds(10);
  static final int MAX_PAYLOAD = 64 * 1024;
  static final String CA_FILE = "ca.crt";
  static final String CERT_FILE = "node.crt";
  static final String KEY_FILE = "node.key";
  static final int MAX_WATCH_TOPIC = 128;
  static final long INITIAL_BACKOFF_MS = 50;
  static final long MAX_BACKOFF_MS = 2000;

  final boolean insecure;
  final Path dataDir;
  final String holder;
  final Duration requestTimeout;
  final Duration readyTimeout;
  final String serverName;

  private Options(
      boolean insecure,
      Path dataDir,
      String holder,
      Duration requestTimeout,
      Duration readyTimeout,
      String serverName) {
    this.insecure = insecure;
    this.dataDir = dataDir;
    this.holder = holder;
    this.requestTimeout = requestTimeout;
    this.readyTimeout = readyTimeout;
    this.serverName = serverName;
  }

  public static Options defaults() {
    return new Options(false, null, "", DEFAULT_REQUEST_TIMEOUT, DEFAULT_READY_TIMEOUT, "");
  }

  /** Plaintext. Also set if {@code CLUSDR_TLS=disabled} and {@code dataDir} is empty. */
  public Options insecure(boolean insecure) {
    return new Options(insecure, dataDir, holder, requestTimeout, readyTimeout, serverName);
  }

  /** Directory with {@code ca.crt} / {@code node.crt} / {@code node.key}. */
  public Options dataDir(Path dataDir) {
    return new Options(insecure, dataDir, holder, requestTimeout, readyTimeout, serverName);
  }

  public Options dataDir(String dataDir) {
    return dataDir(dataDir == null || dataDir.isBlank() ? null : Path.of(dataDir.trim()));
  }

  /** Lock/lease identity. Empty → {@code sdk-<uuid>}. */
  public Options holder(String holder) {
    String h = holder == null ? "" : holder.trim();
    return new Options(insecure, dataDir, h, requestTimeout, readyTimeout, serverName);
  }

  /** Unary timeout (default 10s). */
  public Options requestTimeout(Duration requestTimeout) {
    Duration d =
        requestTimeout == null || requestTimeout.isNegative() || requestTimeout.isZero()
            ? DEFAULT_REQUEST_TIMEOUT
            : requestTimeout;
    return new Options(insecure, dataDir, holder, d, readyTimeout, serverName);
  }

  /** Health wait on connect (default 10s). Zero skips the wait. */
  public Options readyTimeout(Duration readyTimeout) {
    Duration d = readyTimeout == null || readyTimeout.isNegative() ? DEFAULT_READY_TIMEOUT : readyTimeout;
    return new Options(insecure, dataDir, holder, requestTimeout, d, serverName);
  }

  /** TLS server name (peer node id). Else {@code CLUSDR_TLS_SERVER_NAME}, else CN of {@code node.crt}. */
  public Options serverName(String serverName) {
    String n = serverName == null ? "" : serverName.trim();
    return new Options(insecure, dataDir, holder, requestTimeout, readyTimeout, n);
  }

  static String envAddr() {
    String v = getenv("CLUSDR_GRPC_ADDR");
    return v.isEmpty() ? DEFAULT_ADDR : v;
  }

  static boolean envInsecure() {
    String v = getenv("CLUSDR_TLS").toLowerCase();
    return v.equals("disabled") || v.equals("off") || v.equals("false") || v.equals("0");
  }

  static Path envDataDir() {
    String v = getenv("CLUSDR_DATA_DIR");
    if (!v.isEmpty()) {
      return Path.of(v);
    }
    return Path.of(System.getProperty("user.home"), ".clusdr");
  }

  static String envServerName() {
    return getenv("CLUSDR_TLS_SERVER_NAME");
  }

  static String getenv(String key) {
    String v = System.getenv(key);
    return v == null ? "" : v.trim();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Options other)) {
      return false;
    }
    return insecure == other.insecure
        && Objects.equals(dataDir, other.dataDir)
        && holder.equals(other.holder)
        && requestTimeout.equals(other.requestTimeout)
        && readyTimeout.equals(other.readyTimeout)
        && serverName.equals(other.serverName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(insecure, dataDir, holder, requestTimeout, readyTimeout, serverName);
  }
}
