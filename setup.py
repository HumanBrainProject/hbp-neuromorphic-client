#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

requirements = ["requests", "click", "pyyaml", "ebrains_drive"]

test_requirements = [
    # TODO: put package test requirements here
]

long_description = open("README.md").read()

setup(
    name="ebrains_neuromorphic_platform_admin",
    version="0.11.2",
    packages=["nmpi"],
    package_dir={"nmpi": "nmpi"},
    entry_points={"console_scripts": ["nmpi=nmpi.cli:cli"]},
    install_requires=requirements,
    author="Andrew P. Davison and Domenico Guarino",
    author_email="andrew.davison@cnrs.fr",
    description="Client software for administrators of the EBRAINS Neuromorphic Computing Platform",
    long_description=long_description,
    license="License :: OSI Approved :: Apache Software License",
    url="https://github.com/HumanBrainProject/hbp-neuromorphic-client",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: Other/Proprietary License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Scientific/Engineering",
    ],
)
