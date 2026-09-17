# Contributing

This is the Java SDK. The daemon lives in [`clusdr`](https://github.com/clusdr/clusdr). Product docs: [Java SDK](https://clusdr.io/docs/sdk/java).

License: [Apache-2.0](LICENSE). Conduct: [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md). Security: [SECURITY.md](SECURITY.md).

## Commits

Every commit and pull-request title uses [Conventional Commits](https://www.conventionalcommits.org/):

```text
<type>(optional-scope): <imperative summary>
```

Types we use: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `revert`.

Examples:

```text
feat: accept a custom Runtime address in local()
fix: surface Unavailable on a dead socket
docs: point install at the current daemon tag
```

A breaking change uses `feat!:` (or another type with `!`) and a `BREAKING CHANGE:` footer. Subject is lowercase after the type, no trailing period.

CI lints PR commits. Prefer squash-merge; the squash title must stay conventional.

## Requirements

Java 17+. Maven 3.9+. [Buf](https://buf.build/docs/cli/installation) to refresh vendored proto.

```bash
make proto    # export buf.build/clusdr/api, then inject java_package
mvn test
```

`.proto` files live under `proto/`. Do not hand-edit the RPC shapes; export from [`buf.build/clusdr/api`](https://buf.build/clusdr/api) (or sibling `../clusdr/proto/api`). `java_package` / `java_multiple_files` are injected after export so they are not part of the shared wire module. The public API is the Java wrapper, not the generated gRPC shapes.
