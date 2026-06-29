"""Tier-1 unit tests for the HyperSync decode logic in update_chain.

The most safety-critical piece is _decode_log's side->asset mapping: a flip
would silently mislabel every trade. We build synthetic OrderFilled logs with
eth_abi.encode (a real dependency) so decode is checked against known inputs.
"""

import types

import hypersync
import pytest
from eth_abi import encode as abi_encode

from update_utils.update_chain import (
    _DATA_TYPES,
    _as_int,
    _build_query,
    _decode_log,
    _hex_to_bytes,
)


def _topic_addr(addr_40hex: str) -> str:
    """A 20-byte address left-padded to a 32-byte topic, as a hex string."""
    return "0x" + "00" * 12 + addr_40hex


def _make_log(side, token_id, maker_amt, taker_amt,
              maker="aa" * 20, taker="bb" * 20,
              block_number=100, tx_hash="0x" + "cd" * 32):
    data = abi_encode(
        _DATA_TYPES,
        [side, token_id, maker_amt, taker_amt, 0, b"\x00" * 32, b"\x00" * 32],
    )
    return types.SimpleNamespace(
        topics=["0xsig", "0xorderhash", _topic_addr(maker), _topic_addr(taker)],
        data="0x" + data.hex(),
        block_number=block_number,
        transaction_hash=tx_hash,
    )


class TestAsInt:
    def test_int_passthrough(self):
        assert _as_int(123) == 123

    def test_hex_string(self):
        assert _as_int("0x10") == 16

    def test_decimal_string(self):
        assert _as_int("42") == 42

    def test_invalid_raises(self):
        with pytest.raises(TypeError):
            _as_int(1.5)


class TestHexToBytes:
    def test_with_prefix(self):
        assert _hex_to_bytes("0x0a0b") == b"\x0a\x0b"

    def test_without_prefix(self):
        assert _hex_to_bytes("0a0b") == b"\x0a\x0b"


class TestDecodeLog:
    def test_maker_buys_side0(self):
        # side=0 -> maker pays USDC (makerAssetId "0"), receives the outcome token
        log = _make_log(side=0, token_id=12345, maker_amt=3_400_000, taker_amt=5_000_000)
        ts, maker, maker_aid, maker_amt, taker, taker_aid, taker_amt, txh = _decode_log(
            log, {100: 1_700_000_000}
        )
        assert ts == 1_700_000_000
        assert maker == "0x" + "aa" * 20
        assert taker == "0x" + "bb" * 20
        assert maker_aid == "0"
        assert taker_aid == "12345"
        assert maker_amt == "3400000"
        assert taker_amt == "5000000"
        assert txh == "0x" + "cd" * 32

    def test_maker_sells_side1(self):
        # side=1 -> maker gives the token, receives USDC (takerAssetId "0")
        log = _make_log(side=1, token_id=999, maker_amt=5_000_000, taker_amt=3_400_000)
        _, _, maker_aid, _, _, taker_aid, _, _ = _decode_log(log, {100: 1})
        assert maker_aid == "999"
        assert taker_aid == "0"

    def test_addresses_lowercased(self):
        log = _make_log(side=0, token_id=1, maker_amt=1, taker_amt=1,
                        maker="AB" * 20, taker="CD" * 20)
        row = _decode_log(log, {100: 1})
        assert row[1] == "0x" + "ab" * 20
        assert row[4] == "0x" + "cd" * 20

    def test_txhash_gets_0x_prefix(self):
        log = _make_log(side=0, token_id=1, maker_amt=1, taker_amt=1, tx_hash="ef" * 32)
        assert _decode_log(log, {100: 1})[7] == "0x" + "ef" * 32

    def test_block_number_as_hex(self):
        log = _make_log(side=0, token_id=1, maker_amt=1, taker_amt=1, block_number="0x64")
        assert _decode_log(log, {100: 1_234})[0] == 1_234


class TestBuildQuery:
    def test_constructs_query(self):
        # to_block is exclusive in HyperSync, so the helper passes to_block + 1.
        q = _build_query(100, 200)
        assert isinstance(q, hypersync.Query)
