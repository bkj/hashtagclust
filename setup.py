#!/usr/bin/env/python

"""
	Setup script for NDD
"""

from setuptools import setup

setup(
	name='hashclust',
	author='Ben Johnson',
	author_email='ben@gophronesis.com',
	classifiers=[],
	description='hashclust',
	keywords=['hashclust'],
	license='ALV2',
	packages=['hashclust'],
	version="0.0.0",
	entry_points={'console_scripts': ['hashclust = hashclust.__main__:main']},
)
