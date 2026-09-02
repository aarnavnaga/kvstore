# kvstore

A Raft-replicated key-value store in C++20, built on its own LSM-tree storage engine.

Work in progress. See [SPEC.md](SPEC.md) for the plan and [DESIGN.md](DESIGN.md) for the
decisions made along the way.

## Build

Everything builds and runs inside a Linux dev image, so the only local dependency is
Docker.

```bash
make dev-image   # once
make test        # release build + unit tests
make asan        # AddressSanitizer + UBSan
make tsan        # ThreadSanitizer
```
