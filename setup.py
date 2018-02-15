#!/usr/bin/env python

from setuptools import setup

setup(name='tesp',
      version='0.0.1',
      description=('A temporary solution to get packaging underway. '
                   'Code will eventually be ported eo-datasets.'),
      packages=['tesp'],
      install_requires=[
          'click',
          'click_datetime',
          'folium',
          'geopandas',
          'h5py',
          'luigi',
          'numpy',
          'pyyaml',
          'rasterio',
          'scikit-image',
          'shapely',
          'structlog',
          'eodatasets',
          'checksumdir',
          'rios',
          'python-fmask'
      ],
      dependency_links=[
          'git+https://github.com/GeoscienceAustralia/eo-datasets@develop#egg=eodatasets-0.1dev',
          'hg+https://bitbucket.org/chchrsc/rios@1.4.4/#egg=rios-1.4.4',
          'hg+https://bitbucket.org/chchrsc/python-fmask@0.4.5#egg=python-fmask-0.4.5'
      ],
      scripts=['bin/s2package', 'bin/ard_pbs', 'bin/search_s2'],
      include_package_data=True)
