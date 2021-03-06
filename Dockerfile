FROM golang:1.15.3 AS builder

COPY . .

RUN git clone https://github.com/nats-io/stan.go

RUN make build

FROM golang:1.15.3 

COPY --from=builder /tmp/stan-*  /usr/bin/

CMD [ "/bin/bash","-c","while true; do echo running; sleep 1; done"]