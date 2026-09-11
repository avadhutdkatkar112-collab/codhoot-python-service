FROM golang:1.22-bookworm AS builder
WORKDIR /app
COPY go.mod .
COPY main.go .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o codhoot-python-service .

FROM python:3.12-slim-bookworm
WORKDIR /app
COPY --from=builder /app/codhoot-python-service .
ENV PORT=8081
EXPOSE 8081
CMD ["./codhoot-python-service"]
