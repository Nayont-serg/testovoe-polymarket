# Polymarket Wallet History

Восстанавливает on-chain историю баланса кошелька Polymarket на Polygon напрямую
из RPC-логов и проверяет, что вычисленный баланс каждого актива совпадает с `balanceOf` на последнем подтверждённом блоке.

## Настройка

    cp .env.example .env
    uv sync
    make docker-up
    make migrate

Миграции не версионируются. Чтобы сбросить схему:

    docker compose down -v && make docker-up && make migrate

## Запуск

    make run                                                           # WALLET_ADDRESS из .env
    uv run --env-file .env python -m app.main --wallet-address 0x...   # или любой другой кошелёк

## Тесты

    make test              # юнит-тесты, без сети и БД
    make test-integration  # требует `make docker-up` и доступ к сети

Интеграционные тесты используют отдельную БД `polymarket_wallet_history_test`
(переопределяется через `TEST_DATABASE_URL`), создаётся автоматически.
