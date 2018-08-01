#!/usr/bin/env python

from setuptools import setup, find_packages

# There is a bug in the 2.7.6 release of luigi in the luigi.contrib.s3 module
# Using an unreleased version until it is packaged and released


setup(name='tesp',
      version='0.0.5',
      description=('A temporary solution to get packaging underway. '
                   'Code will eventually be ported eo-datasets.'),
      packages=find_packages(),
      install_requires=[
          'click',
          'click_datetime',
          'folium',
          'geopandas',
          'h5py',
          'luigi',
          'numpy',
          'pathlib',
          'pyyaml',
          'rasterio',
          'scikit-image',
          'shapely',
          'structlog',
          'eodatasets',
          'checksumdir',
          'eugl',
      ],
      dependency_links=[
          'git+https://github.com/GeoscienceAustralia/eo-datasets@develop#egg=eodatasets-0.1dev',
          'git+https://github.com/OpenDataCubePipelines/eugl.git#egg=eugl-0.0.2',
          'git+https://github.com/spotify/luigi.git@f9a99dce22e2887406c6d156d5d669660547d257#egg=luigi-2.7.7'
      ],
      scripts=['bin/s2package', 'bin/ard_pbs', 'bin/search_s2'],
      include_package_data=True)
