from app.main import parse_args


def test_parse_args_defaults_wallet_address_to_none() -> None:
    args = parse_args([])
    assert args.wallet_address is None


def test_parse_args_reads_wallet_address_flag() -> None:
    args = parse_args(["--wallet-address", "0xabc"])
    assert args.wallet_address == "0xabc"
