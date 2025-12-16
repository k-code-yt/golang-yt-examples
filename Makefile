build-workers:
	@go build -race -o ./bin/workers ./...
	@chmod +x ./bin/workers

workers: build-workers
	@./bin/workers

