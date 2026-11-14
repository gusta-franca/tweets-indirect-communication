FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY service/ ./service/
RUN CGO_ENABLED=0 go build -o /out/message-service ./service

FROM scratch
COPY --from=build /out/message-service /message-service
ENTRYPOINT ["/message-service"]
