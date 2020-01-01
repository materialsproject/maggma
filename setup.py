#!/usr/bin/env python

import os

from setuptools import setup, find_packages

module_dir = os.path.dirname(os.path.abspath(__file__))

if __name__ == "__main__":
    setup(
        name="maggma",
        use_scm_version=True,
        setup_requires=["setuptools_scm"],
        description="Framework to develop datapipelines from files on disk to full dissemenation API",
        long_description=open(os.path.join(module_dir, "README.md")).read(),
        long_description_content_type="text/markdown",
        url="https://github.com/materialsproject/maggma",
        author="The Materials Project",
        author_email="feedback@materialsproject.org",
        license="modified BSD",
        packages=find_packages(),
        package_data={},
        zip_safe=False,
        install_requires=[
            "pymongo>=3.6",
            "mongomock>=3.10.0",
            "monty>=1.0.2",
            "pydash>=4.1.0",
            "tqdm>=4.19.6",
            "mongogrant>=0.2.2",
        ],
        extras_require={"vault": ["hvac>=0.9.5"], "S3": ["boto3==1.10.10"]},
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
