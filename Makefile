WORKDIR="stan.go"
build:	build-pub	build-sub	build-bench build-bench2
build-pub:
		CGO_ENABLED=0 cd ${WORKDIR} && go build -o /tmp/stan-pub examples/stan-pub/main.go 
build-sub:
		CGO_ENABLED=0 cd ${WORKDIR} && go build -o /tmp/stan-sub examples/stan-sub/main.go 
build-bench:
		CGO_ENABLED=0 cd ${WORKDIR} && go build -o /tmp/stan-bench examples/stan-bench/main.go 
build-bench2:
		CGO_ENABLED=0 cd main && go build -o /tmp/stan-bench2 main.go