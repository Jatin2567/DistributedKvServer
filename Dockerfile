FROM golang:1.21

WORKDIR /app

COPY . .

RUN go build -o kvstore cmd/server/main.go

CMD ["./kvstore", "configs/config.json"]