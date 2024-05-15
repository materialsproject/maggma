# Maggma

[![Static Badge](https://img.shields.io/badge/documentation-blue?logo=github)](https://materialsproject.github.io/maggma) [![testing](https://github.com/materialsproject/maggma/workflows/testing/badge.svg)](https://github.com/materialsproject/maggma/actions?query=workflow%3Atesting) [![codecov](https://codecov.io/gh/materialsproject/maggma/branch/main/graph/badge.svg)](https://codecov.io/gh/materialsproject/maggma) [![python](https://img.shields.io/badge/Python-3.8+-blue.svg?logo=python&amp;logoColor=white)]()

## What is Maggma

Maggma is a framework to build data pipelines from files on disk all the way to a REST API in scientific environments. Maggma has been developed by the Materials Project (MP) team at Lawrence Berkeley National Laboratory.

Maggma is written in [Python](http://docs.python-guide.org/en/latest/) and supports Python 3.8+.

## Installation from PyPI

Maggma is published on the [Python Package Index](https://pypi.org/project/maggma/).  The preferred tool for installing
packages from *PyPi* is **pip**.  This tool is provided with all modern
versions of Python.

Open your terminal and run the following command.

``` shell
pip install --upgrade maggma
```

## Installation from source

You can install Maggma directly from a clone of the [Git repository](https://github.com/materialsproject/maggma).  This can be done either by cloning the repo and installing from the local clone, or simply installing directly via **git**.

=== "Local Clone"

    ``` shell
    git clone https://github.com//materialsproject/maggma
    cd maggma
    python setup.py install
    ```

=== "Direct Git"
    ``` shell
    pip install git+https://github.com/materialsproject/maggma
    ```
