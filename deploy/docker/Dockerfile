# Production image: kvserver, kvcli and the chaos history recorder on a slim base.
# Build:  docker build -t kvstore -f deploy/docker/Dockerfile .

FROM ubuntu:24.04 AS build
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      clang-18 lld-18 cmake ninja-build libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*
ENV CC=clang-18 CXX=clang++-18
WORKDIR /src
COPY CMakeLists.txt ./
COPY src ./src
RUN cmake -S . -B /build -G Ninja -DCMAKE_BUILD_TYPE=Release -DKV_BUILD_TESTS=OFF \
    && cmake --build /build --target kvserver kvcli kvhistory \
    && strip /build/src/kvserver /build/src/kvcli /build/src/kvhistory

FROM ubuntu:24.04
ARG DEBIAN_FRONTEND=noninteractive
# iproute2 (tc netem) and iptables are for fault injection; libfaketime for clock skew.
RUN apt-get update && apt-get install -y --no-install-recommends \
      iproute2 iptables libfaketime tini ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home /var/lib/kvstore kvstore
COPY --from=build /build/src/kvserver /build/src/kvcli /build/src/kvhistory /usr/local/bin/
COPY deploy/docker/entrypoint.sh /usr/local/bin/entrypoint.sh
EXPOSE 7000 9100
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/entrypoint.sh"]
CMD ["kvserver"]
