FROM golang:1.18 AS builder

ARG GIT_COMMIT
WORKDIR /
RUN git clone https://github.com/stellar/go.git stellar-go
WORKDIR /stellar-go

# By default "git fetch" only fetches refs/<branchname>
# Below ensures we also fetch PR refs
RUN git config --add remote.origin.fetch "+refs/pull/*/head:refs/remotes/origin/pull/*"
RUN git fetch --force --quiet origin
RUN if [ -n "${GIT_COMMIT}" ]; then git checkout "${GIT_COMMIT}"; fi
RUN go build -v ./exp/lighthorizon/index/cmd/batch/map
RUN go build -v ./exp/lighthorizon/index/cmd/batch/reduce
RUN go build -v ./exp/lighthorizon/index/cmd/single

FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive
# ca-certificates are required to make tls connections
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl wget gnupg apt-utils
RUN apt-get clean

COPY --from=builder /stellar-go/map /usr/local/bin/
COPY --from=builder /stellar-go/reduce /usr/local/bin/
COPY --from=builder /stellar-go/single /usr/local/bin/

