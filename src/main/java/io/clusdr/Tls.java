package io.clusdr;

import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.TlsChannelCredentials;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Locale;

final class Tls {
  private Tls() {}

  static ManagedChannel channel(String addr, Options opts) {
    boolean insecure = opts.insecure || (Options.envInsecure() && opts.dataDir == null);
    if (insecure) {
      return Grpc.newChannelBuilder(addr, InsecureChannelCredentials.create()).build();
    }
    TlsMaterial material = clientCredentials(opts);
    return Grpc.newChannelBuilder(addr, material.credentials)
        .overrideAuthority(material.serverName)
        .build();
  }

  static TlsMaterial clientCredentials(Options opts) {
    Path directory = opts.dataDir != null ? opts.dataDir : Options.envDataDir();
    PemFiles files = loadFiles(directory);
    if (files == null) {
      throw new ClusdrException(
          "clusdr: TLS enabled but "
              + Options.CA_FILE
              + "/"
              + Options.CERT_FILE
              + "/"
              + Options.KEY_FILE
              + " missing in "
              + directory
              + "; set CLUSDR_TLS=disabled or pass insecure(true)");
    }
    ChannelCredentials creds;
    try (InputStream ca = new ByteArrayInputStream(files.ca);
        InputStream cert = new ByteArrayInputStream(files.cert);
        InputStream key = new ByteArrayInputStream(files.key)) {
      creds = TlsChannelCredentials.newBuilder().trustManager(ca).keyManager(cert, key).build();
    } catch (IOException e) {
      throw new ClusdrException("clusdr: TLS credentials: " + e.getMessage(), e);
    }
    String name = firstNonBlank(opts.serverName, Options.envServerName(), cnFromPem(files.cert));
    if (name.isEmpty()) {
      throw new ClusdrException(
          "clusdr: TLS hostname unknown; set CLUSDR_TLS_SERVER_NAME or serverName to the peer node id");
    }
    return new TlsMaterial(creds, name);
  }

  static String cnFromPem(byte[] pem) {
    try {
      CertificateFactory cf = CertificateFactory.getInstance("X.509");
      X509Certificate cert =
          (X509Certificate) cf.generateCertificate(new ByteArrayInputStream(pem));
      String dn = cert.getSubjectX500Principal().getName();
      for (String part : dn.split(",")) {
        String p = part.trim();
        if (p.toUpperCase(Locale.ROOT).startsWith("CN=")) {
          return p.substring(3).trim();
        }
      }
      return "";
    } catch (Exception e) {
      return "";
    }
  }

  private static PemFiles loadFiles(Path directory) {
    Path ca = directory.resolve(Options.CA_FILE);
    Path cert = directory.resolve(Options.CERT_FILE);
    Path key = directory.resolve(Options.KEY_FILE);
    if (!Files.isRegularFile(ca) || !Files.isRegularFile(cert) || !Files.isRegularFile(key)) {
      return null;
    }
    try {
      return new PemFiles(Files.readAllBytes(ca), Files.readAllBytes(cert), Files.readAllBytes(key));
    } catch (IOException e) {
      throw new ClusdrException("clusdr: TLS files: " + e.getMessage(), e);
    }
  }

  private static String firstNonBlank(String... values) {
    for (String v : values) {
      if (v != null && !v.isBlank()) {
        return v.trim();
      }
    }
    return "";
  }

  static final class TlsMaterial {
    final ChannelCredentials credentials;
    final String serverName;

    TlsMaterial(ChannelCredentials credentials, String serverName) {
      this.credentials = credentials;
      this.serverName = serverName;
    }
  }

  private record PemFiles(byte[] ca, byte[] cert, byte[] key) {}
}
