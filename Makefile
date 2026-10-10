# Every target runs inside the Linux dev image so epoll, sanitizers and libFuzzer behave
# the same on a Mac laptop as in CI.
IMAGE    ?= kvstore-dev
DOCKER   ?= docker
RUN      := $(DOCKER) run --rm -t -v $(CURDIR):/src -w /src $(IMAGE)
JOBS     ?= $(shell nproc 2>/dev/null || sysctl -n hw.ncpu)

.PHONY: dev-image build test asan tsan fmt fmt-check tidy shell clean

dev-image:
	$(DOCKER) build -t $(IMAGE) -f deploy/docker/Dockerfile.dev deploy/docker

# build-<mode> configures and builds one of: release, asan, tsan.
build-%:
	$(RUN) sh -c 'cmake -S . -B build/$* -G Ninja $(CMAKE_FLAGS_$*) && cmake --build build/$* -j$(JOBS)'

CMAKE_FLAGS_release := -DCMAKE_BUILD_TYPE=RelWithDebInfo
CMAKE_FLAGS_asan    := -DCMAKE_BUILD_TYPE=Debug -DKV_SANITIZER=address
CMAKE_FLAGS_tsan    := -DCMAKE_BUILD_TYPE=RelWithDebInfo -DKV_SANITIZER=thread
CMAKE_FLAGS_fuzz    := -DCMAKE_BUILD_TYPE=RelWithDebInfo -DKV_BUILD_FUZZ=ON -DKV_BUILD_TESTS=OFF

build: build-release

test-%: build-%
	$(RUN) ctest --test-dir build/$* --output-on-failure -j$(JOBS) $(CTEST_ARGS)

test: test-release
asan: test-asan
tsan: test-tsan

fmt:
	$(RUN) sh -c "find src tests fuzz bench chaos \( -name '*.cc' -o -name '*.h' \) | xargs -r clang-format -i"

fmt-check:
	$(RUN) sh -c "find src tests fuzz bench chaos \( -name '*.cc' -o -name '*.h' \) | xargs -r clang-format --dry-run --Werror"

tidy: build-release
	$(RUN) sh -c "find src -name '*.cc' | xargs -r clang-tidy -p build/release --quiet --use-color=false"

shell:
	$(DOCKER) run --rm -it -v $(CURDIR):/src -w /src $(IMAGE) bash

clean:
	rm -rf build

# ---- images, chaos, Kubernetes ----------------------------------------------------------

.PHONY: image scan chaos chaos-matrix k8s-e2e k8s-down

image:
	$(DOCKER) build -t kvstore:dev -f deploy/docker/Dockerfile .

# Fails on any critical CVE in the production image.
scan: image
	$(DOCKER) run --rm -v /var/run/docker.sock:/var/run/docker.sock aquasec/trivy:0.56.2 \
	  image --quiet --scanners vuln --severity CRITICAL --exit-code 1 kvstore:dev

# One fault-injection scenario: make chaos SCENARIO=leader-crash SEED=1
chaos: image
	chaos/run.sh $(SCENARIO) $(or $(SEED),1)

# Every scenario x 20 seeds (about two hours).
chaos-matrix: image
	chaos/matrix.sh 1 20

k8s-e2e: image
	deploy/k8s/e2e.sh

k8s-down:
	deploy/k8s/e2e.sh down
