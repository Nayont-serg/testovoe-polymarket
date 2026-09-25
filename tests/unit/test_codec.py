from app.rpc.codec import (
    TRANSFER_BATCH_TOPIC,
    TRANSFER_TOPIC,
    address_topic,
    decode_erc20_transfer,
    decode_string_return,
    decode_transfer_batch,
    decode_transfer_single,
    decode_uint256,
    encode_balance_of_erc20_call,
    encode_balance_of_erc1155_call,
)


def test_address_topic_pads_to_32_bytes():
    topic = address_topic("0x46b353667fd7d846af3bbeda6584b0e5b883d3de")
    assert topic == "0x00000000000000000000000046b353667fd7d846af3bbeda6584b0e5b883d3de"
    assert len(topic) == 66


def test_encode_balance_of_erc20_call_uses_selector_and_padded_address():
    data = encode_balance_of_erc20_call("0x46b353667fd7d846af3bbeda6584b0e5b883d3de")
    assert data == (
        "0x70a0823100000000000000000000000046b353667fd7d846af3bbeda6584b0e5b883d3de"
    )


def test_encode_balance_of_erc1155_call_appends_position_id_word():
    data = encode_balance_of_erc1155_call(
        "0x46b353667fd7d846af3bbeda6584b0e5b883d3de", 5
    )
    assert data.startswith("0x00fdd58e")
    assert data.endswith("0" * 63 + "5")


def test_decode_erc20_transfer_reads_from_to_and_amount():
    topics = [
        TRANSFER_TOPIC,
        "0x0000000000000000000000001111111111111111111111111111111111111111",
        "0x0000000000000000000000002222222222222222222222222222222222222222",
    ]
    data = "0x" + format(1_000_000, "064x")
    from_address, to_address, amount = decode_erc20_transfer(topics, data)
    assert from_address == "0x1111111111111111111111111111111111111111"
    assert to_address == "0x2222222222222222222222222222222222222222"
    assert amount == 1_000_000


def test_decode_transfer_single_reads_operator_from_to_id_amount():
    topics = [
        "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62",
        "0x0000000000000000000000001111111111111111111111111111111111111111",
        "0x0000000000000000000000002222222222222222222222222222222222222222",
        "0x0000000000000000000000003333333333333333333333333333333333333333",
    ]
    data = "0x" + format(42, "064x") + format(7, "064x")
    operator, from_address, to_address, position_id, amount = decode_transfer_single(
        topics, data
    )
    assert operator == "0x1111111111111111111111111111111111111111"
    assert from_address == "0x2222222222222222222222222222222222222222"
    assert to_address == "0x3333333333333333333333333333333333333333"
    assert position_id == 42
    assert amount == 7


def test_decode_transfer_batch_reads_id_value_pairs():
    topics = [
        TRANSFER_BATCH_TOPIC,
        "0x0000000000000000000000001111111111111111111111111111111111111111",
        "0x0000000000000000000000002222222222222222222222222222222222222222",
        "0x0000000000000000000000003333333333333333333333333333333333333333",
    ]
    data = (
        "0x"
        "0000000000000000000000000000000000000000000000000000000000000040"
        "00000000000000000000000000000000000000000000000000000000000000a0"
        "0000000000000000000000000000000000000000000000000000000000000002"
        "0000000000000000000000000000000000000000000000000000000000000001"
        "0000000000000000000000000000000000000000000000000000000000000002"
        "0000000000000000000000000000000000000000000000000000000000000002"
        "0000000000000000000000000000000000000000000000000000000000000064"
        "00000000000000000000000000000000000000000000000000000000000000c8"
    )
    operator, from_address, to_address, pairs = decode_transfer_batch(topics, data)
    assert operator == "0x1111111111111111111111111111111111111111"
    assert from_address == "0x2222222222222222222222222222222222222222"
    assert to_address == "0x3333333333333333333333333333333333333333"
    assert pairs == [(1, 100), (2, 200)]


def test_decode_string_return_reads_abi_encoded_short_string():
    data = (
        "0x"
        "0000000000000000000000000000000000000000000000000000000000000020"
        "0000000000000000000000000000000000000000000000000000000000000004"
        "5553444300000000000000000000000000000000000000000000000000000000"
    )
    assert decode_string_return(data) == "USDC"


def test_decode_uint256_parses_hex_word():
    assert decode_uint256("0x" + format(123456, "064x")) == 123456
