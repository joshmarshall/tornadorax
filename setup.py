#!/usr/bin/env python

from setuptools import setup, find_packages


with open("VERSION") as version_fp:
    VERSION = version_fp.read().strip()

setup(
    name="tornadorax",
    version=VERSION,
    description="Tornado wrapper for OpenStack services",
    author='Josh Marshall',
    author_email='catchjosh@gmail.com',
    url="http://github.com/joshmarshall/tornadorax/",
    license="http://www.apache.org/licenses/LICENSE-2.0",
    packages=find_packages(exclude=["tests", "dist"]),
    install_requires=["tornado", "python-dateutil"]
)
