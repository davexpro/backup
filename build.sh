#!/usr/bin/env bash

export CGO_ENABLED=0 GOOS=linux GOARCH=amd64

go build -ldflags "-s -w -extldflags '-static' -X main.magic=${GIT_SHA} -X main.date=${DATE}" -o backup ./cmd/backup/main.go
if command -v upx >/dev/null 2>&1; then
    echo "[*] found upx, now compress the bin."
    upx -9 backup
fi