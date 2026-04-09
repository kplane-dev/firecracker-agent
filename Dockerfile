FROM golang:1.25 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /firecracker-agent ./cmd/agent

FROM scratch
COPY --from=builder /firecracker-agent /firecracker-agent
ENTRYPOINT ["/firecracker-agent"]
