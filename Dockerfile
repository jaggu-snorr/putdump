FROM golang:1.24.2-alpine3.21 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
RUN go build

FROM scratch
COPY --from=build /src/putdump /putdump

ENTRYPOINT ["/putdump"]

