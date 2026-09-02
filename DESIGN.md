# Design

Decisions, the alternatives that lost, and the bugs that changed the design. Each section
is added in the milestone that made the decision.

## Build and toolchain (M0)

**Build inside a Linux container, even on a Mac.** The network layer is built on epoll,
and ThreadSanitizer, libFuzzer and `perf` are all first-class on Linux and partial on
macOS. A `Makefile` wraps every target in `docker run` against `deploy/docker/Dockerfile.dev`,
so a laptop and CI run the same compiler (Clang 18), the same standard library and the
same sanitizers.

**Dependencies come from the OS image, not vcpkg.** The spec suggests vcpkg. Every
dependency so far (GoogleTest, OpenSSL) is packaged by Ubuntu 24.04, and installing them
with `apt` keeps a cold CI build under a minute instead of compiling them from source.
CMake falls back to fetching GoogleTest if it is not installed, so a non-Ubuntu machine
still builds.

**Three build modes on every push.** `release`, `asan` (AddressSanitizer + UBSan) and
`tsan` (ThreadSanitizer) run the full test suite in CI. Any sanitizer report fails the
build (`halt_on_error=1`). ASan and TSan cannot be combined in one binary, which is why
they are separate jobs.

## Bug log

| # | Milestone | Symptom | Cause | Caught by |
|---|---|---|---|---|
