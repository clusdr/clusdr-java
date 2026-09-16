package io.clusdr;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

final class Retry {
  private Retry() {}

  static boolean isTransient(Throwable err) {
    Status.Code code = Status.fromThrowable(err).getCode();
    return code == Status.Code.UNAVAILABLE
        || code == Status.Code.RESOURCE_EXHAUSTED
        || code == Status.Code.ABORTED;
  }

  static boolean readyTransient(Throwable err) {
    return isTransient(err) || Status.fromThrowable(err).getCode() == Status.Code.DEADLINE_EXCEEDED;
  }

  static <T> T retry(Callable<T> fn, long deadlineMs) {
    return retry(fn, deadlineMs, Retry::isTransient, () -> false);
  }

  static <T> T retry(Callable<T> fn, long deadlineMs, Predicate<Throwable> transientCheck, java.util.function.BooleanSupplier closed) {
    long backoff = Options.INITIAL_BACKOFF_MS;
    Throwable last = null;
    for (; ; ) {
      if (closed.getAsBoolean()) {
        if (last instanceof RuntimeException re) {
          throw re;
        }
        throw new ClusdrException("clusdr: aborted", last);
      }
      long remaining = deadlineMs - System.currentTimeMillis();
      if (remaining <= 0) {
        if (last != null) {
          sneaky(last);
        }
        throw new ClusdrException("clusdr: request timeout");
      }
      try {
        return fn.call();
      } catch (Exception err) {
        if (!transientCheck.test(err)) {
          sneaky(err);
        }
        last = err;
      }
      long wait = Math.min(backoff, Math.max(0, deadlineMs - System.currentTimeMillis()));
      if (wait <= 0) {
        if (last != null) {
          sneaky(last);
        }
        throw new ClusdrException("clusdr: request timeout");
      }
      try {
        Thread.sleep(wait);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        sneaky(last != null ? last : ie);
      }
      backoff = Math.min(backoff * 2, Options.MAX_BACKOFF_MS);
    }
  }

  static long remainingMs(long deadlineMs) {
    long left = deadlineMs - System.currentTimeMillis();
    if (left <= 0) {
      throw new ClusdrException("clusdr: request timeout");
    }
    return left;
  }

  static boolean cancelled(Throwable err) {
    return Status.fromThrowable(err).getCode() == Status.Code.CANCELLED;
  }

  static StatusRuntimeException status(Throwable err) {
    if (err instanceof StatusRuntimeException s) {
      return s;
    }
    return Status.fromThrowable(err).asRuntimeException();
  }

  @SuppressWarnings("unchecked")
  static <E extends Throwable> void sneaky(Throwable t) throws E {
    throw (E) t;
  }
}
