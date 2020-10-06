#!/usr/bin/env python

from setuptools import setup, find_packages


setup(
    name="tesp",
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    url="https://github.com/OpenDataCubePipelines/tesp",
    description="Data Pipeline construction.",
    packages=find_packages(exclude=("tests",)),
    install_requires=[
        "click",
        "click_datetime",
        "ciso8601",
        "folium",
        "geopandas",
        "h5py",
        "luigi>2.7.6",
        "numpy",
        "pyyaml",
        "rasterio",
        "scikit-image",
        "shapely",
        "structlog",
        "checksumdir",
        "eodatasets",
        "eugl",
        "wagl",
        "importlib-metadata;python_version<'3.8'",
    ],
    extras_require=dict(
        test=["pytest", "pytest-flake8", "deepdiff", "flake8", "pep8-naming"]
    ),
    dependency_links=[
        "git+https://github.com/GeoscienceAustralia/eo-datasets.git@develop#egg=eodatasets",
        "git+https://github.com/GeoscienceAustralia/wagl@develop#egg=wagl",
        "git+https://github.com/OpenDataCubePipelines/eugl.git@master#egg=eugl",
    ],
    scripts=[
        "bin/s2package",
        "bin/ard_pbs",
        "bin/search_s2",
        "bin/s2-nci-processing",
        "bin/batch_summary",
    ],
    include_package_data=True,
)
