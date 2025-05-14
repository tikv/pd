FROM hub.pingcap.net/keyspace/builder/golang:1.23-bullseye as builder

RUN apt update && apt install -y make git curl gcc g++ unzip

# Setup ssh key for private deps
ARG GITHUB_TOKEN
RUN if [ -n "$GITHUB_TOKEN" ]; then \
        git config --global url."https://${GITHUB_TOKEN}@github.com/".insteadOf "https://github.com/"; \
    fi

RUN mkdir -p /go/src/github.com/tikv/pd
WORKDIR /go/src/github.com/tikv/pd

# Cache dependencies
COPY go.mod .
COPY go.sum .

RUN GO111MODULE=on go mod download

COPY . .

# Skip swagger generation.
ENV SWAGGER=0
# Skip building dashboard.
ENV DASHBOARD=0

RUN make

FROM hub.pingcap.net/keyspace/builder/debian:bullseye-slim
RUN apt update && apt install -y jq bash curl dnsutils wget && rm /bin/sh && ln -s /bin/bash /bin/sh && \
    apt-get clean autoclean && apt-get autoremove --yes

COPY --from=builder /go/src/github.com/tikv/pd/bin/pd-server /pd-server
COPY --from=builder /go/src/github.com/tikv/pd/bin/pd-ctl /pd-ctl
COPY --from=builder /go/src/github.com/tikv/pd/bin/pd-recover /pd-recover

EXPOSE 2379 2380

ENTRYPOINT ["/pd-server"]
