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
	install_requires=[
		"ultrajson>=1.35",
		"scipy>=0.18.1",
		"numpy>=1.11.2",
		"kafka>=1.3.1",
		"twutils"
	],
	dependency_links=[
		"git+ssh://git@github.com/bkj/twutils.git"
	]
)
