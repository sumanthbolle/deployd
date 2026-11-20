# Build stage: compile a static binary.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/deployd ./cmd/deployd

# Runtime stage: distroless has no shell or package manager, and runs as a
# non-root user, which keeps the attack surface small.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/deployd /deployd
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/deployd"]
