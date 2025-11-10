build:
	@go build -o bin/rl ./...
	@chmod +x ./bin/rl

run: build
	@./bin/rl

run-race: 
	@go clean -testcache
	@go test -race -v -run TestRateLimiter .
 	