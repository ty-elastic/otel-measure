FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod .
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o otel-measurement .

FROM scratch
COPY --from=builder /app/otel-measurement /otel-measurement
EXPOSE 8080
ENTRYPOINT ["/otel-measurement"]
