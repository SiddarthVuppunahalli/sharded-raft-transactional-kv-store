# Builds kvnode, kvctl and kvbench into one small image. The runtime image
# includes iproute2 (tc netem) and iptables so fault-injection scripts can
# add latency / partitions inside each container.
FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/kvnode ./cmd/kvnode \
 && CGO_ENABLED=0 go build -o /out/kvctl ./cmd/kvctl \
 && CGO_ENABLED=0 go build -o /out/kvbench ./cmd/kvbench

FROM alpine:3.22
RUN apk add --no-cache iproute2 iptables bash
COPY --from=build /out/ /usr/local/bin/
EXPOSE 7000
ENTRYPOINT ["kvnode"]
