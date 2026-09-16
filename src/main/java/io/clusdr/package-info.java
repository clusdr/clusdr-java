/**
 * Application SDK for the local Clusdr daemon.
 *
 * <p>The application is not a cluster member. It dials the daemon on this host
 * the same way a process talks to a local Docker engine. This package never
 * dials other nodes and never joins Raft.
 *
 * <pre>{@code
 * your process  ──►  clusdr daemon on this host  ──►  the rest of the cluster
 * }</pre>
 *
 * <pre>{@code
 * try (Cluster c = Clusdr.local()) {
 *   List<Member> members = c.members();
 * }
 * }</pre>
 *
 * <p>TLS is on unless {@code Options.insecure(true)} or {@code CLUSDR_TLS=disabled}.
 * Missing PEMs are an error (no skip-verify fallback). Unary calls retry
 * UNAVAILABLE / ABORTED / RESOURCE_EXHAUSTED.
 *
 * <p>Walkthrough: <a href="https://clusdr.io/docs/sdk/java">clusdr.io/docs/sdk/java</a>
 */
package io.clusdr;
