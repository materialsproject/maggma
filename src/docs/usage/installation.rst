=================
Installing Maggma
=================

.. contents::
   :depth: 1
   :local:
   :backlinks: none

.. highlight:: console

Overview
--------

Maggma is written in `Python`__ and supports Python 3.6+.

__ http://docs.python-guide.org/en/latest/

Installation from PyPI
----------------------

Maggma is published on the `Python Package Index
<https://pypi.org/project/maggma/>`_.  The preferred tool for installing
packages from *PyPI* is :command:`pip`.  This tool is provided with all modern
versions of Python.

Open your terminal and run the following command.

::

   $ pip install -U maggma

Installation from source
------------------------

You can install Maggma directly from a clone of the `Git repository`__.  This
can be done either by cloning the repo and installing from the local clone, or
simply installing directly via :command:`git`.

::

   $ git clone https://github.com/sphinx-doc/sphinx
   $ cd sphinx
   $ pip install .

::

   $ pip install git+https://github.com/materialsproject/sphinx

__ https://github.com/materialsproject/maggma
