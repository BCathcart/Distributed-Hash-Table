FROM --platform=${BUILDPLATFORM} golang:1.15 AS build
WORKDIR /${HOME}/cpen431/miniproject
ENV CGO_ENGABLED=0
COPY go.* .
RUN go mod download
COPY . .
ARG TARGETOS
ARG TARGETARCH
RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o /out/server ./src/server 

FROM scratch AS bin
COPY --from=build /out/server /
ENTRYPOINT [ "/out/server" ]
CMD ["--help"]