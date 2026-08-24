FROM golang:1.22-alpine AS build

WORKDIR /app
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN go build -o /kvstore ./cmd/kvstore
RUN go build -o /democtl ./cmd/democtl

FROM alpine:3.20 AS runtime

COPY --from=build /kvstore /kvstore
EXPOSE 8080
ENTRYPOINT ["/kvstore"]

FROM alpine:3.20 AS democtl

RUN apk add --no-cache bash curl docker-cli docker-cli-compose

COPY --from=build /democtl /democtl
EXPOSE 9090
ENTRYPOINT ["/democtl"]
