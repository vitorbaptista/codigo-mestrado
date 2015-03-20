# -*- coding: utf-8 -*-

from setuptools import setup

from pipeline import __version__


setup(
    name='pipeline',
    version=__version__,
    packages=['pipeline', 'pipeline.metrics'],
    author='Vitor Baptista',
    author_email='vitor@vitorbaptista.com',
    test_suite='pipeline.test',
    license='MIT',
)
