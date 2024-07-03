SUDO := $(shell docker version >/dev/null 2>&1 || echo "sudo")

# https://github.com/opencontainers/image-spec/blob/master/annotations.md
BUILD_DATE := $(shell date -u +%FT%TZ)
VERSION ?= $(shell git rev-parse --short HEAD)
DOCKER_IMAGE := stellar/ledger-exporter

docker-build:
	cd ../../../ && \
	$(SUDO) docker build --platform linux/amd64 --pull --label org.opencontainers.image.created="$(BUILD_DATE)" \
    --build-arg GOFLAGS="-ldflags=-X=github.com/stellar/go/exp/services/ledgerexporter/internal.version=$(VERSION)" \
$(if $(STELLAR_CORE_VERSION), --build-arg STELLAR_CORE_VERSION=$(STELLAR_CORE_VERSION)) \
	-f exp/services/ledgerexporter/docker/Dockerfile \
	-t $(DOCKER_IMAGE):$(VERSION) \
	-t $(DOCKER_IMAGE):latest .

docker-push:
	$(SUDO) docker push $(DOCKER_IMAGE):$(VERSION)
	$(SUDO) docker push $(DOCKER_IMAGE):latest
