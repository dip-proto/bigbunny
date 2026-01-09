.PHONY: build test test-v lint fmt clean run

build:
	go build ./cmd/bbd

test:
	go test ./...

test-v:
	go test -v ./...

test-store:
	go test ./test -run TestStore -v

test-cluster:
	go test ./test -run Cluster -v

test-clock:
	go test ./test -run ClockSkew -v

test-soak:
	SOAK=1 go test ./test -run Soak -v

lint:
	go vet ./...

fmt:
	gofmt -w .

clean:
	rm -f bbd
	rm -rf tmp/

run:
	go run ./cmd/bbd --dev --host-id=node1 --tcp=:8081 --uds=/tmp/bbd.sock
