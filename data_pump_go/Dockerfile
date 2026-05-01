# Multi-stage Dockerfile for data_pump_go
FROM golang:1.22-alpine AS builder

# Install build dependencies
RUN apk add --no-cache build-base git

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build static binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o data_pump_go .

# Runtime stage
FROM alpine:3.19

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder /app/data_pump_go .

ENTRYPOINT ["/app/data_pump_go"]
