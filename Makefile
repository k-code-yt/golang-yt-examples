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

test-app-race:
	@go clean -testcache
	@go test -race -v ./...
	