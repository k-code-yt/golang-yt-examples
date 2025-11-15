build-app:
	@go build -o ./bin/app ./...
	@chmod +x ./bin/app

app: build-app
	@./bin/app

test-app-race:
	@go clean -testcache
	@go test -race -v ./...
	