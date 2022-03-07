#!/usr/bin/env python


from pathlib import Path

from setuptools import find_packages, setup

module_dir = Path(__file__).resolve().parent

with open(module_dir / "README.md") as f:
    long_desc = f.read()
setup(
    name="maggma",
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    description="Framework to develop datapipelines from files on disk to full dissemenation API",
    long_description=long_desc,
    long_description_content_type="text/markdown",
    url="https://github.com/materialsproject/maggma",
    author="The Materials Project",
    author_email="feedback@materialsproject.org",
    license="modified BSD",
    packages=find_packages("src"),
    package_dir={"": "src"},
    package_data={"maggma": ["py.typed"]},
    zip_safe=False,
    include_package_data=True,
    install_requires=[
        "setuptools",
        "pymongo>=4.0",
        "monty>=1.0.2",
        "mongomock>=3.10.0",
        "pydash>=4.1.0",
        "jsonschema>=3.1.1",
        "tqdm>=4.19.6",
        "mongogrant>=0.3.1",
        "aioitertools>=0.5.1",
        "numpy>=1.17.3",
        "pydantic>=0.32.2",
        "fastapi>=0.42.0",
        "pyzmq==22.3.0",
        "dnspython>=1.16.0",
        "sshtunnel>=0.1.5",
        "msgpack>=0.5.6",
        "orjson>=3.6.0",
        "boto3>=1.20.41",
    ],
    extras_require={
        "vault": ["hvac>=0.9.5"],
        "montydb": ["montydb>=2.3.12"],
        "notebook_runner": ["IPython>=7.16", "nbformat>=5.0", "regex>=2020.6"],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Science/Research",
        "Intended Audience :: System Administrators",
        "Intended Audience :: Information Technology",
        "Operating System :: OS Independent",
        "Topic :: Other/Nonlisted Topic",
        "Topic :: Database :: Front-Ends",
        "Topic :: Scientific/Engineering",
    ],
    entry_points={"console_scripts": ["mrun = maggma.cli:run"]},
    tests_require=["pytest"],
    python_requires=">=3.7",
)
