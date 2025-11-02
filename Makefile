build-chat:
	@go build -o ./bin/chat ./...
	@chmod +x ./bin/chat

chat: build-chat
	@./bin/chat

test-chat-race:
	@go clean -testcache
	@go test -race -v ./...
	
test-rooms-race:
	@go clean -testcache
	@go test -race -v -timeout 30s -run TestRooms .
	
test-chat:
	@go clean -testcache
	@go test -v ./...
