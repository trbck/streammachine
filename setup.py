"""
StreamMachine setup configuration.

Cython extensions are optional and compiled with:
    pip install -e ".[cython]"
    python setup.py build_ext --inplace

If Cython is not installed or compilation fails, pure Python
fallbacks are used automatically.
"""
from setuptools import setup, find_packages, Extension
import os
import sys

# Check if Cython is available
CYTHON_AVAILABLE = False
try:
    from Cython.Build import cythonize
    CYTHON_AVAILABLE = True
except ImportError:
    cythonize = None  # type: ignore

# Determine if we should build Cython extensions
# Build if: CYTHON_AVAILABLE and (installing with [cython] or BUILD_CYTHON env var)
BUILD_CYTHON = CYTHON_AVAILABLE and (
    os.environ.get("BUILD_CYTHON", "").lower() in ("1", "true", "yes") or
    "[cython]" in " ".join(sys.argv) or
    any("cython" in arg.lower() for arg in sys.argv)
)

# Define Cython extensions
ext_modules = []
if BUILD_CYTHON and CYTHON_AVAILABLE:
    # Compiler flags - use -O2 for broader compatibility
    extra_compile_args = ["-O2"]
    extra_link_args = []

    # Skip -march=native for compatibility with Rosetta and cross-compile scenarios

    ext_modules = [
        Extension(
            "streammachine.cython.cython_decode",
            ["src/streammachine/cython/cython_decode.pyx"],
            extra_compile_args=extra_compile_args,
            extra_link_args=extra_link_args,
        ),
        Extension(
            "streammachine.cython.fast_ohlc",
            ["src/streammachine/cython/fast_ohlc.pyx"],
            extra_compile_args=extra_compile_args,
            extra_link_args=extra_link_args,
        ),
        Extension(
            "streammachine.cython.fast_consumer",
            ["src/streammachine/cython/fast_consumer.pyx"],
            extra_compile_args=extra_compile_args,
            extra_link_args=extra_link_args,
        ),
    ]

    ext_modules = cythonize(
        ext_modules,
        compiler_directives={
            "language_level": "3",
            "boundscheck": False,
            "wraparound": False,
            "cdivision": True,
            "initializedcheck": False,
        },
    )

setup(
    name="streammachine",
    version="0.1.0",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[
        "coredis>=4.22.0",
        "venusian",
        "uvloop",
    ],
    python_requires=">=3.8",
    ext_modules=ext_modules,
)
