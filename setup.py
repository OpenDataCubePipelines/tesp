#!/usr/bin/env python

from setuptools import setup, find_packages
import versioneer


setup(name='tesp',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      url='https://github.com/OpenDataCubePipelines/tesp',
      description=('A temporary solution to get packaging underway. '
                   'Code will eventually be ported eo-datasets.'),
      packages=find_packages(),
      install_requires=[
          'click',
          'click_datetime',
          'ciso8601',
          'folium',
          'geopandas',
          'h5py',
          'luigi>2.7.6',
          'numpy',
          'pathlib',
          'pyyaml',
          'rasterio',
          'scikit-image',
          'shapely',
          'structlog',
          'checksumdir',
          'eodatasets',
          'eugl',
          'wagl',
      ],
      extras_require=dict(
          test=[
              'pytest',
              'pytest-flake8',
              'deepdiff',
              'flake8',
              'pep8-naming',
          ],
      ),
      dependency_links=[
          'git+https://github.com/GeoscienceAustralia/eo-datasets.git@develop#egg=eodatasets',
          'git+https://github.com/GeoscienceAustralia/wagl@master#egg=wagl',
          'git+https://github.com/OpenDataCubePipelines/eugl.git@master#egg=eugl',
      ],

      scripts=['bin/s2package',
               'bin/ard_pbs',
               'bin/search_s2',
               'bin/s2-nci-processing',
               'bin/batch_summary'],
      include_package_data=True
)
