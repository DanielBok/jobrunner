.PHONY: start stop

start:
	docker compose up -d

stop:
	docker compose down
