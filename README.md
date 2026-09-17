<h1 align="center">
  <a href="https://clusdr.io/docs/sdk/java">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/clusdr/clusdr-java/main/assets/java-lettermark-dark.svg">
      <img src="https://raw.githubusercontent.com/clusdr/clusdr-java/main/assets/java-lettermark.svg" alt="clusdr Java" width="160" height="164">
    </picture>
  </a>
</h1>

<p align="center">A runtime for the cluster. An SDK for the app.</p>

<p align="center">
  <a href="https://clusdr.io/docs/sdk/java"><img src="https://img.shields.io/badge/docs-clusdr.io-0C0C10" alt="docs"></a>
  <a href="https://central.sonatype.com/artifact/io.clusdr/clusdr"><img src="https://img.shields.io/maven-central/v/io.clusdr/clusdr" alt="Maven Central"></a>
  <a href="https://github.com/clusdr/clusdr-java/actions/workflows/ci.yml"><img src="https://github.com/clusdr/clusdr-java/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/clusdr/clusdr-java/blob/main/pom.xml"><img src="https://img.shields.io/badge/java-17%2B-green" alt="Java 17+"></a>
  <a href="https://github.com/clusdr/clusdr-java/blob/main/LICENSE"><img src="https://img.shields.io/github/license/clusdr/clusdr-java" alt="License"></a>
</p>

Application SDK for the **local** Clusdr daemon. This package does not join the cluster.

```text
your process  ──►  clusdr daemon on this host  ──►  the rest of the cluster
```

Not a database, queue, or Kubernetes. Wire API is **v1alpha1**. TLS is on by default. Blocking client on Java 17+.

## Install

```xml
<dependency>
  <groupId>io.clusdr</groupId>
  <artifactId>clusdr</artifactId>
  <version>0.2.0</version>
</dependency>
```

Same version train as the daemon. A running daemon on this host is required:

```bash
curl -fsSL https://clusdr.io/install.sh | sh
```

Linux amd64/arm64, or [Docker Hub `durguto/clusdr`](https://hub.docker.com/r/durguto/clusdr) (GHCR: `ghcr.io/clusdr/clusdr`). Then `clusdr init` and `clusdr start --bootstrap`. Guide: [first member](https://clusdr.io/docs/guide/first-member).

## Use

`Clusdr.local()` dials `CLUSDR_GRPC_ADDR` or `127.0.0.1:7947`, waits on Health, then you own the connection.

```java
try (Cluster c = Clusdr.local()) {
  List<Member> members = c.members();
  Member leader = c.leader();

  c.publish("deployment", Map.of("sha", "abc"));

  Lock lk = c.lock("scheduler", Duration.ofSeconds(15));
  c.unlock("scheduler");

  for (Event event : c.watch()) {
    // member.join, leader.changed, custom.deployment, …
    break;
  }
}
```

`dial` is for tests and operators. Apps use `local()`.

One `Cluster` is safe for concurrent unary calls. Same connection = same holder (`unlock` is process-wide for that name). Several `watch()` loops on one client are fine.

`ttl` is a `Duration`. `close()` (and try-with-resources) stops Watch, unlocks, and revokes what this process still holds. Failures throw `ClusdrException`.

Full surface: [Java SDK](https://clusdr.io/docs/sdk/java). Runnable copies (Go, Python, Rust, TypeScript, and Java): [examples](https://github.com/clusdr/clusdr/tree/main/examples).

## TLS

On unless `insecure(true)` or `CLUSDR_TLS=disabled`. PEMs (`ca.crt`, `node.crt`, `node.key`) come from `dataDir`, `CLUSDR_DATA_DIR`, or `~/.clusdr`. Missing files are an error; this client does not skip-verify.

## Not in this package

- Join, promote, or configure the cluster (CLI)
- Talking to a remote node's Runtime API as the normal path — put a daemon on that host

## Links

- **Docs:** [clusdr.io](https://clusdr.io) · [Java SDK](https://clusdr.io/docs/sdk/java) · [from your app](https://clusdr.io/docs/guide/from-your-app)
- **Daemon:** [github.com/clusdr/clusdr](https://github.com/clusdr/clusdr)
- **This repo:** [github.com/clusdr/clusdr-java](https://github.com/clusdr/clusdr-java)

Apache-2.0. Contributor checkout: [CONTRIBUTING.md](CONTRIBUTING.md).
