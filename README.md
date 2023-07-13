# tesp [![Build Status](https://travis-ci.org/OpenDataCubePipelines/tesp.svg?branch=master)](https://travis-ci.org/OpenDataCubePipelines/tesp)

OpenDataCubePipeline is the code for the DEA's Optical ARD processing of the following satellites:
 * Landsat 5
 * Landsat 7
 * Landsat 8
 * Landsat 9
 * Sentinel 2A
 * Sentinel 2B

Pre-commit setup
----------------

A [pre-commit](https://pre-commit.com/) config is provided to automatically format
and check your code changes. This allows you to immediately catch and fix
issues before you raise a failing pull request (which run the same checks under
Travis).

If you don't use Conda, install pre-commit from pip:

    pip install pre-commit

If you do use Conda, install from conda-forge (*required* because the pip
version uses virtualenvs which are incompatible with Conda's environments)

    conda install pre_commit

Now install the pre-commit hook to the current repository:

    pre-commit install

Your code will now be formatted and validated before each commit. You can also
invoke it manually by running `pre-commit run`

Module creation
----------------
The tesp code is built into the ard-pipeline module.  This module is used to generate ARD on NCI. Instructions and code to build this module are stored in the [dea-wagl-docker](https://bitbucket.org/geoscienceaustralia/dea-wagl-docker/src/master/ard-pipeline/) repository.
