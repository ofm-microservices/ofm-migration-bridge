FROM golang:1.25 AS build
WORKDIR /src
COPY ofm-common /src/ofm-common
COPY ofm-migration-bridge /src/ofm-migration-bridge
WORKDIR /src/ofm-migration-bridge
RUN CGO_ENABLED=0 go build -mod=mod -o /out/migration-bridge ./cmd/migration-bridge

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/migration-bridge /migration-bridge
ENTRYPOINT ["/migration-bridge"]
