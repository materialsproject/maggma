# Maggma

[![linting](https://github.com/materialsproject/maggma/workflows/linting/badge.svg)](https://github.com/materialsproject/maggma/actions?query=workflow%3Alinting) [![testing](https://github.com/materialsproject/maggma/workflows/testing/badge.svg)](https://github.com/materialsproject/maggma/actions?query=workflow%3Atesting) [![codecov](https://codecov.io/gh/materialsproject/maggma/branch/main/graph/badge.svg)](https://codecov.io/gh/materialsproject/maggma) [![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/materialsproject/maggma.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/materialsproject/maggma/context:python)

## What is Maggma

Maggma is a framework to build data pipelines from files on disk all the way to a REST API in scientific environments. Maggma has been developed by the Materials Project (MP) team at Lawrence Berkeley National Laboratory.

Maggma is written in [Python](http://docs.python-guide.org/en/latest/) and supports Python 3.7+.

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
