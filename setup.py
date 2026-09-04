"""Empacotamento do código-fonte como wheel para deploy via Databricks Asset Bundles."""

from setuptools import find_packages, setup

setup(
    name="data_engineering",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[],
)
