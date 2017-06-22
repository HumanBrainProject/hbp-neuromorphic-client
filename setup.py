#!/usr/bin/env python
# -*- coding: utf-8 -*-

from distutils.core import setup

long_description = open("README.md").read()

setup(
    name="hbp_neuromorphic_platform",
    version='0.5.2',
    packages=['nmpi'],
    install_requires=['requests',],
    author="Andrew P. Davison and Domenico Guarino",
    author_email="andrew.davison@unic.cnrs-gif.fr",
    description="Client software for the Human Brain Project Neuromorphic Computing Platform",
    long_description=long_description,
    license="License :: OSI Approved :: Apache Software License",
    url='http://www.humanbrainproject.eu',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'License :: Other/Proprietary License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Topic :: Scientific/Engineering']
)
