build-app:
	@go build -o ./bin/app ./cmd/.
	@chmod +x ./bin/app

app: build-app
	@./bin/app

build-app-race:
	@go build -race -o ./bin/app ./cmd/.
	@chmod +x ./bin/app

app-race: build-app-race
	@./bin/app
	
kafka-app: build-app
	@CG_ID=$(CG_ID) SHOULD_PRODUCE=$(SHOULD_PRODUCE) ./bin/app

kafka-c-1: build-app-race
	@CG_ID=consumer-group-1 SHOULD_PRODUCE=false ./bin/app

kafka-c-2: build-app-race
	@CG_ID=consumer-group-2 SHOULD_PRODUCE=false ./bin/app

kafka-p: build-app-race
	@CG_ID=producer-group SHOULD_PRODUCE=true ./bin/app

test-app-race:
	@go clean -testcache
	@go test -race -v ./...
	