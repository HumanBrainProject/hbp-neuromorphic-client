#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

requirements = [
    'requests',
    'click',
    'pyyaml'
]

test_requirements = [
    # TODO: put package test requirements here
]

long_description = open("README.md").read()

setup(
    name="hbp_neuromorphic_platform",
    version='0.10.1',
    packages=['nmpi'],
    package_dir={'nmpi': 'nmpi'},
    entry_points={
        'console_scripts': [
            'nmpi=nmpi.cli:cli'
        ]
    },
    install_requires=requirements,
    author="Andrew P. Davison and Domenico Guarino",
    author_email="andrew.davison@cnrs.fr",
    description="Client software for the EBRAINS Neuromorphic Computing Platform",
    long_description=long_description,
    license="License :: OSI Approved :: Apache Software License",
    url='http://www.humanbrainproject.eu',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'License :: Other/Proprietary License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.8',
        'Topic :: Scientific/Engineering']
)
