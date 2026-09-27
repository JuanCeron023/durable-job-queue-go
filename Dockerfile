FROM node:20-alpine AS frontend
WORKDIR /fe
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm install
COPY frontend/ ./
RUN npm run build

FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=frontend /web/static ./web/static
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/tickflow ./cmd/tickflow

FROM alpine:3.20
RUN adduser -D -u 10001 app && mkdir -p /data && chown app /data
WORKDIR /app
COPY --from=build /out/tickflow /app/tickflow
COPY --from=build /src/web /app/web
USER app
VOLUME ["/data"]
EXPOSE 8081
ENTRYPOINT ["/app/tickflow", "--http", ":8081", "--journal", "/data/tickflow.journal"]
