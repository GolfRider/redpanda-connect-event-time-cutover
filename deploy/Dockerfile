# Build from the repository root: docker build -f deploy/Dockerfile .
FROM golang:1.27.3 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /redpanda-connect-cutover ./cmd/redpanda-connect-cutover

FROM gcr.io/distroless/static-debian12
COPY --from=build /redpanda-connect-cutover /redpanda-connect-cutover
ENTRYPOINT ["/redpanda-connect-cutover"]
