FROM --platform=$BUILDPLATFORM golang:1.22-alpine AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod main.go ./
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -ldflags="-s -w" -o /edge-task .
RUN head -c 262144000 /dev/urandom > /ballast

FROM scratch AS light
COPY --from=build /edge-task /edge-task
ENTRYPOINT ["/edge-task"]
CMD ["--sleep", "1s"]

FROM light AS heavy
COPY --from=build /ballast /ballast
