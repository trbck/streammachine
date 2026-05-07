import time
import random
import string

try:
    from streammachine.cython import decode_dict_bytes_to_utf8, _has_cython_decode
    has_cython = _has_cython_decode
except ImportError:
    decode_dict_bytes_to_utf8 = None
    has_cython = False

try:
    import ujson
    has_ujson = True
except ImportError:
    has_ujson = False

try:
    import orjson
    has_orjson = True
except ImportError:
    has_orjson = False

def py_decode_dict_bytes_to_utf8(d):
    return {k.decode("utf-8"): v.decode("utf-8") for k, v in d.items()}

def ujson_decode_dict_bytes_to_utf8(d):
    # Convert to {str: str} via utf-8, then encode/decode via ujson
    s = ujson.dumps({k.decode("utf-8"): v.decode("utf-8") for k, v in d.items()})
    return ujson.loads(s)

def orjson_decode_dict_bytes_to_utf8(d):
    # Convert to {str: str} via utf-8, then encode/decode via orjson
    s = orjson.dumps({k.decode("utf-8"): v.decode("utf-8") for k, v in d.items()})
    return orjson.loads(s)

def random_bytes_dict(n, keylen=8, vallen=16):
    """Generate a dict of n random bytes->bytes pairs."""
    out = {}
    for _ in range(n):
        k = ''.join(random.choices(string.ascii_letters, k=keylen)).encode('utf-8')
        v = ''.join(random.choices(string.ascii_letters + string.digits, k=vallen)).encode('utf-8')
        out[k] = v
    return out

def benchmark():
    N = 100_000  # Number of items
    d = random_bytes_dict(N)
    print(f"Benchmarking decode of {N} bytes->bytes pairs...")

    # Python
    t0 = time.perf_counter()
    py_result = py_decode_dict_bytes_to_utf8(d)
    t1 = time.perf_counter()
    print(f"Pure Python: {t1-t0:.4f} seconds")

    # Cython
    if has_cython:
        t0 = time.perf_counter()
        cy_result = decode_dict_bytes_to_utf8(d)
        t1 = time.perf_counter()
        print(f"Cython:      {t1-t0:.4f} seconds")
        assert py_result == cy_result
    else:
        print("Cython extension not available.")

    # ujson
    if has_ujson:
        t0 = time.perf_counter()
        uj_result = ujson_decode_dict_bytes_to_utf8(d)
        t1 = time.perf_counter()
        print(f"ujson:       {t1-t0:.4f} seconds")
        assert py_result == uj_result
    else:
        print("ujson not available.")

    # orjson
    if has_orjson:
        t0 = time.perf_counter()
        oj_result = orjson_decode_dict_bytes_to_utf8(d)
        t1 = time.perf_counter()
        print(f"orjson:      {t1-t0:.4f} seconds")
        assert py_result == oj_result
    else:
        print("orjson not available.")

if __name__ == "__main__":
    benchmark() 