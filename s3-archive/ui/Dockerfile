# Stock temporalio/ui at a pinned commit + s3-archive.patch.
ARG UI_COMMIT=2d3303c666fa6be624d94d1510ee42e67d366c2e

FROM node:22-alpine AS web
ARG UI_COMMIT
RUN apk add --no-cache git && corepack enable
WORKDIR /ui
RUN git init -q && git remote add origin https://github.com/temporalio/ui.git \
 && git fetch -q --depth 1 origin ${UI_COMMIT} && git checkout -q FETCH_HEAD
COPY s3-archive.patch /tmp/s3-archive.patch
RUN git apply /tmp/s3-archive.patch
RUN pnpm install --frozen-lockfile --config.engine-strict=false
RUN VITE_API= BUILD_PATH=server/ui/assets/local pnpm exec vite build

FROM golang:1.26-alpine AS server
COPY --from=web /ui/server /server
WORKDIR /server
RUN GOTOOLCHAIN=auto go build -o ui-server ./cmd/server/main.go

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata bash \
 && addgroup -g 1000 temporal && adduser -u 1000 -G temporal -D temporal
WORKDIR /home/ui-server
COPY --from=server --chown=temporal:temporal /server/ui-server ./
COPY --from=server --chown=temporal:temporal /server/config/docker.yaml ./config/docker.yaml
USER temporal
EXPOSE 8080
ENTRYPOINT ["./ui-server", "--env", "docker", "start"]
