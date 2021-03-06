# -*- coding: utf-8 -*-

from setuptools import setup

from pipeline import __version__


setup(
    name='pipeline',
    version=__version__,
    packages=['pipeline', 'pipeline.metrics'],
    scripts=['bin/rice_index', 'bin/rollmean', 'bin/breakout_detection',
             'bin/parties_and_coalitions_changes', 'bin/votes_to_csv'],
    test_suite='pipeline.test',

    install_requires=[
        'numpy>=0.9',
        'pandas>=0.15',
        'SQLAlchemy>=0.9.8',
    ],

    author='Vitor Baptista',
    author_email='vitor@vitorbaptista.com',
    license='MIT',
)
