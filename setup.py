#!/usr/bin/env python
# -*- coding: utf-8 -*-

VERSION = '0.1'
#
import sys
import os
from setuptools import setup, find_packages

setup(
    name='streamengine',
    version=VERSION,
    description='streamengine - Python Stream Processing With Redis Streams',
    url='http://github.com/trbck/streamengine',
    author='trbck',
    packages=find_packages(),
    license='GNU General Public License v3.0',
    zip_safe=False)
