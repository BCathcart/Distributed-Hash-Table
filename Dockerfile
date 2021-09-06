FROM golang:1.15.6-alpine AS build
WORKDIR /src
ENV CGO_ENGABLED=0
COPY go.* ./
RUN go mod download
COPY *.txt /etc/dht/
COPY . .
ARG TARGETOS
ARG TARGETARCH
RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -mod=readonly -o /out/dht-server ./src/server

FROM alpine AS bin
ENV GOTRACEBACK=single
WORKDIR /src
COPY --from=build /out/dht-server .
COPY --from=build /etc/dht/peers.txt /etc/dht/peers.txt
