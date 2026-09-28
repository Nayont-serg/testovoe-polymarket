.PHONY: run test test-integration lint migrate docker-up docker-down

run:
	uv run --env-file .env python -m app.main

test:
	uv run pytest tests/unit

test-integration:
	uv run pytest -m integration

lint:
	uv run ruff check .
	uv run ruff format --check .

migrate:
	uv run --env-file .env python -m app.db.migrate

docker-up:
	docker compose up -d --wait

docker-down:
	docker compose down
