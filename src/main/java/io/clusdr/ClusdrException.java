package io.clusdr;

/** A Clusdr daemon or SDK failure. */
public final class ClusdrException extends RuntimeException {
  public ClusdrException(String message) {
    super(message);
  }

  public ClusdrException(String message, Throwable cause) {
    super(message, cause);
  }

  static ClusdrException wrap(String prefix, Throwable err) {
    if (err instanceof ClusdrException e) {
      return e;
    }
    String msg = err.getMessage() != null ? err.getMessage() : String.valueOf(err);
    return new ClusdrException(prefix + ": " + msg, err);
  }
}
