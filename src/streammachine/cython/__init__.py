"""
Cython-accelerated modules for StreamMachine.

This module provides optimized functions for:
- Decoding Redis stream data (decode_dict_bytes_to_utf8)
- OHLC aggregation (FastOHLC)
- Fast stream consumption (FastStreamConsumer)

If Cython extensions are not compiled, pure Python fallbacks are used.
"""

# Core decode function
try:
    from .cython_decode import decode_dict_bytes_to_utf8
    _has_cython_decode = True
except ImportError:
    decode_dict_bytes_to_utf8 = None
    _has_cython_decode = False

# Fast OHLC aggregation
try:
    from .fast_ohlc import (
        FastOHLC as FastOHLC_Cython,
        CandleView,
        parse_stream_id_timestamp,
        format_candle_for_redis,
    )
    _has_fast_ohlc = True
except ImportError:
    FastOHLC_Cython = None  # type: ignore
    CandleView = None  # type: ignore
    parse_stream_id_timestamp = None  # type: ignore
    format_candle_for_redis = None  # type: ignore
    _has_fast_ohlc = False

# Fast stream consumer
try:
    from .fast_consumer import (
        FastStreamConsumer,
        ParsedMessage,
        parse_stream_entries,
    )
    _has_fast_consumer = True
except ImportError:
    FastStreamConsumer = None  # type: ignore
    ParsedMessage = None  # type: ignore
    parse_stream_entries = None  # type: ignore
    _has_fast_consumer = False

__all__ = [
    # Core decode
    'decode_dict_bytes_to_utf8',
    '_has_cython_decode',
    # Fast OHLC
    'FastOHLC_Cython',
    'CandleView',
    'parse_stream_id_timestamp',
    'format_candle_for_redis',
    '_has_fast_ohlc',
    # Fast consumer
    'FastStreamConsumer',
    'ParsedMessage',
    'parse_stream_entries',
    '_has_fast_consumer',
]