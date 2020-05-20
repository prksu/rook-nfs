# Build the provisioner binary
FROM golang:1.13.8 as builder
WORKDIR /workspace

# Run this with docker build --build_arg $(go env GOPROXY) to override the goproxy
ARG goproxy=https://proxy.golang.org
ENV GOPROXY=$goproxy

# Copy the Go Modules manifests
COPY go.mod go.sum ./
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the sources
COPY ./ ./

# Build
RUN CGO_ENABLED=0 GOOS=linux GO111MODULE=on go build -a -ldflags '-extldflags "-static"' -o provisioner cmd/provisioner/main.go

# Use distroless as minimal base image to package the provisioner binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM docker.io/ubuntu:focal
WORKDIR /
COPY --from=builder /workspace/provisioner .

ENTRYPOINT ["/provisioner"]