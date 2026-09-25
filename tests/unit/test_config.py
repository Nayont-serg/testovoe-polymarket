from app.config import Settings


def test_settings_from_env_parses_comma_separated_url_lists():
    env = {
        "DATABASE_URL": "postgresql://u:p@localhost:5433/db",
        "LOG_RPC_URLS": "https://a.example,https://b.example",
        "RECEIPT_RPC_URLS": "https://c.example",
        "CALL_RPC_URLS": "https://d.example,https://e.example",
    }
    settings = Settings.from_env(env)
    assert settings.log_rpc_urls == ("https://a.example", "https://b.example")
    assert settings.receipt_rpc_urls == ("https://c.example",)
    assert settings.call_rpc_urls == ("https://d.example", "https://e.example")
    assert settings.wallet_address == "0x46b353667fd7d846af3bbeda6584b0e5b883d3de"


def test_settings_from_env_applies_numeric_defaults():
    env = {
        "DATABASE_URL": "postgresql://u:p@localhost:5433/db",
        "LOG_RPC_URLS": "https://a.example",
        "RECEIPT_RPC_URLS": "https://c.example",
        "CALL_RPC_URLS": "https://d.example",
    }
    settings = Settings.from_env(env)
    assert settings.free_log_rpc_window_blocks == 200_000
    assert settings.goldsky_raw_log_rpc_concurrency_start == 60
    assert settings.receipt_rpc_use_proxy is False
