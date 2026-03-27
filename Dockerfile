FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG CMD=server
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/service ./cmd/${CMD}

FROM gcr.io/distroless/static-debian12
COPY --from=builder /bin/service /service
EXPOSE 8080
ENTRYPOINT ["/service"]
