.PHONY: all build down down-clean stop rebuild rebuild-clean go-build lint

all: build

build:
	docker-compose up --build -d

down:
	docker-compose down

down-clean:
	docker-compose down -v

stop:
	docker-compose stop

rebuild: down build

rebuild-clean: down-clean build

go-build:
	go build ./...

lint:
	golangci-lint run ./...