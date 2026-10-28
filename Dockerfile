FROM golang:1.25 AS build

WORKDIR /app

COPY . .

RUN CGO_ENABLED=0 go build -o /kvstore ./cmd/server && CGO_ENABLED=0 go build -o /demo ./cmd/demo

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /kvstore /demo ./
COPY configs ./configs
EXPOSE 8080
CMD ["./demo"]
