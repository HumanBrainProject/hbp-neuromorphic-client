#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup
import os

long_description = open("README.txt").read()

setup(
    name = "nmpi_client",
    version = '0.1.0dev',
    packages = ['nmpi'],
    scripts = ['bin/nmpi_saga.py'],
    install_requires=['requests',],
    author = "Andrew P. Davison and Domenico Guarino",
    author_email = "andrew.davison@unic.cnrs-gif.fr",
    description = "Client software for the Human Brain Project Neuromorphic Computing Platform",
    long_description = long_description,
    license = "Proprietary License",
    url='http://www.humanbrainproject.eu',
    classifiers = [
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Science/Research',
        'License :: Other/Proprietary License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
                   'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
                   'Programming Language :: Python :: 3.3',
                   'Programming Language :: Python :: 3.4',
        'Topic :: Scientific/Engineering']
)
