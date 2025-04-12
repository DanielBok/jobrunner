.PHONY: start stop

build:
	# for local development
	go build -v -o jrctl.exe

start:
	docker compose up -d

stop:
	docker compose down
