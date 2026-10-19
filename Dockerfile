# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/loadtest ./cmd/loadtest

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/server /out/loadtest /app/
COPY config/rules.yaml /app/config/rules.yaml
ENV RULES_FILE=/app/config/rules.yaml LISTEN_ADDR=:50051 METRICS_ADDR=:9090
EXPOSE 50051 9090
USER nonroot:nonroot
ENTRYPOINT ["/app/server"]
