FROM golang:1.26-alpine3.23 AS builder
RUN apk add --no-cache make git
ARG BUILD_VERSION
ENV BUILD_VERSION=${BUILD_VERSION}
ADD --chown=1001:0 . /evmconnect
WORKDIR /evmconnect
RUN mkdir /.cache \
  && chgrp -R 0 /.cache \
  && chmod -R g+rwX /.cache
USER 1001
RUN make

# Copy the migrations from FFTM down into our local migrations directory
RUN DB_MIGRATIONS_DIR=$(go list -f '{{.Dir}}' github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi | sed 's|pkg/ffcapi|db|') \
  && cp -R "$DB_MIGRATIONS_DIR" db

FROM alpine:3.21.3
WORKDIR /evmconnect
RUN addgroup -g 1001 evmgroup && adduser -D -u 1001 -G evmgroup evmuser
RUN chgrp -R 0 /evmconnect \
  && chmod -R g+rwX /evmconnect
RUN apk add --no-cache curl jq
RUN curl -sL "https://github.com/golang-migrate/migrate/releases/download/$(curl -sL https://api.github.com/repos/golang-migrate/migrate/releases/latest | jq -r '.tag_name')/migrate.linux-amd64.tar.gz" | tar xz \
  && chmod +x ./migrate \
  && mv ./migrate /usr/bin/migrate
COPY --from=builder --chown=1001:0 /evmconnect/firefly-evmconnect /usr/bin/evmconnect
COPY --from=builder --chown=1001:0 /evmconnect/db/ /evmconnect/db/
USER 1001

ENTRYPOINT [ "/usr/bin/evmconnect" ]
