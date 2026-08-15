FROM golang:1.22-alpine AS build

WORKDIR /app
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN go build -o /kvstore ./cmd/kvstore

FROM alpine:3.20

COPY --from=build /kvstore /kvstore
EXPOSE 8080
ENTRYPOINT ["/kvstore"]
