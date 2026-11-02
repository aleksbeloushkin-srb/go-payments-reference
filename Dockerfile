# Build both binaries in a full Go image, ship them in a distroless image
# that has no shell, no package manager and runs as a non-root user.
FROM golang:1.24-alpine AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=local

# No separate "go mod download" layer: it would fetch every module in go.mod,
# including the test-only ones (testcontainers and the Docker client). go
# build downloads just the dozen modules the binaries import.
COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /out/payments ./cmd/payments \
 && go build -trimpath -ldflags="-s -w" -o /out/fakegateway ./cmd/fakegateway

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/payments /out/fakegateway /
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/payments"]
