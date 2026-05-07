# cython: language_level=3
from cpython.bytes cimport PyBytes_AsStringAndSize
from cpython.unicode cimport PyUnicode_FromStringAndSize

def decode_dict_bytes_to_utf8(data):
    """
    Decode a dict of bytes->bytes to str->str as fast as possible.
    """
    cdef dict out = {}
    cdef bytes k, v
    cdef char *k_ptr, *v_ptr
    cdef Py_ssize_t k_len, v_len
    for k, v in data.items():
        PyBytes_AsStringAndSize(k, &k_ptr, &k_len)
        PyBytes_AsStringAndSize(v, &v_ptr, &v_len)
        out[PyUnicode_FromStringAndSize(k_ptr, k_len)] = PyUnicode_FromStringAndSize(v_ptr, v_len)
    return out 