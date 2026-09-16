package io.clusdr;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TlsTest {
  private static final byte[] NODE_A_CERT =
      ("""
      -----BEGIN CERTIFICATE-----
      MIIBmTCCAT+gAwIBAgIUQcxWr82x7fO3L8Jl+Gdnhg2XsrQwCgYIKoZIzj0EAwIw
      IjEPMA0GA1UECgwGY2x1c2RyMQ8wDQYDVQQDDAZub2RlLWEwHhcNMjYwOTEyMDk0
      MjE0WhcNMjYwOTEzMDk0MjE0WjAiMQ8wDQYDVQQKDAZjbHVzZHIxDzANBgNVBAMM
      Bm5vZGUtYTBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABPT6vDCA4Rz1uoJqDDRn
      Oek6f1d/DUKHL6EphIpdG7Nt5G8BflHU0EorNZp5mU0T+NHkF88RjUOQdXO9N58d
      Q5yjUzBRMB0GA1UdDgQWBBRRcJ28BTllqHsVyBLoyIiEIhvrgTAfBgNVHSMEGDAW
      gBRRcJ28BTllqHsVyBLoyIiEIhvrgTAPBgNVHRMBAf8EBTADAQH/MAoGCCqGSM49
      BAMCA0gAMEUCIBSuV03b6SI3b0La8HerkVb4jsDiKG711hB8jhV/X6v1AiEAsMIB
      70nN7/saTVsIuZDacHOF+kM+mYqPN33lUdIukXk=
      -----END CERTIFICATE-----
      """)
          .getBytes(StandardCharsets.US_ASCII);

  @Test
  void cnFromPem() {
    assertEquals("node-a", Tls.cnFromPem(NODE_A_CERT));
  }

  @Test
  void missingCertsError(@TempDir Path dir) {
    ClusdrException err =
        assertThrows(
            ClusdrException.class,
            () -> Tls.clientCredentials(Options.defaults().dataDir(dir)));
    assertTrue(err.getMessage().contains("TLS enabled"));
  }
}
