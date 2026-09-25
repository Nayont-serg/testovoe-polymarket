from __future__ import annotations

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
TRANSFER_SINGLE_TOPIC = (
    "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
)
TRANSFER_BATCH_TOPIC = (
    "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"
)
POSITION_SPLIT_TOPIC = (
    "0x2e6bb91f8cbcda0c93623c54d0403a43514fabc40084ec96b6d5379a74786298"
)
POSITIONS_MERGE_TOPIC = (
    "0x6f13ca62553fcc2bcd2372180a43949c1e4cebba603901ede2f4e14f36b282ca"
)
PAYOUT_REDEMPTION_TOPIC = (
    "0x2682012a4a4f1973119f1c9b90745d1bd91fa2bab387344f044cb3586864d18d"
)

BALANCE_OF_ERC20_SELECTOR = "0x70a08231"
BALANCE_OF_ERC1155_SELECTOR = "0x00fdd58e"
SYMBOL_SELECTOR = "0x95d89b41"
DECIMALS_SELECTOR = "0x313ce567"


def normalize_address(address: str) -> str:
    return address.lower()


def address_topic(address: str) -> str:
    return "0x" + normalize_address(address)[2:].rjust(64, "0")


def decode_address_from_topic(topic: str) -> str:
    return normalize_address("0x" + topic[-40:])


def decode_uint256(word_hex: str) -> int:
    return int(word_hex, 16)


def encode_balance_of_erc20_call(address: str) -> str:
    return BALANCE_OF_ERC20_SELECTOR + address_topic(address)[2:]


def encode_balance_of_erc1155_call(address: str, position_id: int) -> str:
    return (
        BALANCE_OF_ERC1155_SELECTOR
        + address_topic(address)[2:]
        + format(position_id, "064x")
    )


def decode_erc20_transfer(topics: list[str], data: str) -> tuple[str, str, int]:
    from_address = decode_address_from_topic(topics[1])
    to_address = decode_address_from_topic(topics[2])
    amount = decode_uint256(data)
    return from_address, to_address, amount


def decode_transfer_single(
    topics: list[str], data: str
) -> tuple[str, str, str, int, int]:
    operator = decode_address_from_topic(topics[1])
    from_address = decode_address_from_topic(topics[2])
    to_address = decode_address_from_topic(topics[3])
    body = data[2:]
    position_id = int(body[0:64], 16)
    amount = int(body[64:128], 16)
    return operator, from_address, to_address, position_id, amount


def _word(body: str, word_index: int) -> int:
    start = word_index * 64
    return int(body[start : start + 64], 16)


def decode_transfer_batch(
    topics: list[str], data: str
) -> tuple[str, str, str, list[tuple[int, int]]]:
    operator = decode_address_from_topic(topics[1])
    from_address = decode_address_from_topic(topics[2])
    to_address = decode_address_from_topic(topics[3])
    body = data[2:]
    ids_offset = _word(body, 0) // 32
    values_offset = _word(body, 1) // 32
    ids_length = _word(body, ids_offset)
    ids = [_word(body, ids_offset + 1 + i) for i in range(ids_length)]
    values_length = _word(body, values_offset)
    values = [_word(body, values_offset + 1 + i) for i in range(values_length)]
    return operator, from_address, to_address, list(zip(ids, values))


def decode_string_return(data: str) -> str:
    body = data[2:]
    length = _word(body, 1)
    hex_chars = body[128 : 128 + length * 2]
    return bytes.fromhex(hex_chars).decode("utf-8")
