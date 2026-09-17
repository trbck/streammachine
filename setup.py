"""
Optional Cython extensions for StreamMachine.

All project metadata lives in pyproject.toml. This file only exists to
compile the optional accelerators, and only when explicitly requested:

    STREAMMACHINE_BUILD_CYTHON=1 pip install "streammachine[cython]"

Without that variable a pure-Python wheel is built and the pure-Python
fallbacks in the package are used at runtime.
"""
import os
from pathlib import Path

from setuptools import Extension, setup

BUILD_CYTHON = os.environ.get("STREAMMACHINE_BUILD_CYTHON", "").lower() in ("1", "true", "yes")

ext_modules = []
if BUILD_CYTHON:
    try:
        from Cython.Build import cythonize
    except ImportError as exc:  # pragma: no cover - build-time only
        raise SystemExit(
            "STREAMMACHINE_BUILD_CYTHON=1 requires Cython: pip install cython"
        ) from exc

    cython_dir = Path("src/streammachine/cython")
    ext_modules = cythonize(
        [
            Extension(
                f"streammachine.cython.{pyx.stem}",
                [str(pyx)],
                extra_compile_args=["-O2"],
            )
            for pyx in sorted(cython_dir.glob("*.pyx"))
        ],
        compiler_directives={
            "language_level": "3",
            "boundscheck": False,
            "wraparound": False,
            "cdivision": True,
            "initializedcheck": False,
        },
    )

setup(ext_modules=ext_modules)
