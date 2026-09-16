package io.clusdr;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

class RetryTest {
  @Test
  void transientCodes() {
    assertFalse(Retry.isTransient(new RuntimeException("x")));
    assertTrue(Retry.isTransient(rpc(Status.Code.UNAVAILABLE)));
    assertTrue(Retry.isTransient(rpc(Status.Code.ABORTED)));
    assertTrue(Retry.isTransient(rpc(Status.Code.RESOURCE_EXHAUSTED)));
    assertFalse(Retry.isTransient(rpc(Status.Code.INVALID_ARGUMENT)));
  }

  @Test
  void retrySucceedsAfterTransient() {
    int[] n = {0};
    String out =
        Retry.retry(
            () -> {
              n[0] += 1;
              if (n[0] < 3) {
                throw rpc(Status.Code.UNAVAILABLE);
              }
              return "ok";
            },
            System.currentTimeMillis() + 2000);
    assertEquals("ok", out);
    assertEquals(3, n[0]);
  }

  @Test
  void retryStopsOnDeadline() {
    StatusRuntimeException err =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                Retry.retry(
                    () -> {
                      throw rpc(Status.Code.UNAVAILABLE);
                    },
                    System.currentTimeMillis() + 150));
    assertTrue(Retry.isTransient(err));
  }

  @Test
  void retryDoesNotRetryInvalidArgument() {
    StatusRuntimeException err =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                Retry.retry(
                    () -> {
                      throw rpc(Status.Code.INVALID_ARGUMENT);
                    },
                    System.currentTimeMillis() + 2000));
    assertEquals(Status.Code.INVALID_ARGUMENT, err.getStatus().getCode());
  }

  private static StatusRuntimeException rpc(Status.Code code) {
    return Status.fromCode(code).withDescription("x").asRuntimeException();
  }
}
