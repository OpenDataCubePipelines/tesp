from unittest import TestClass
from collections import namedtuple

import numpy
from tesp.package import get_cogtif_options
from deepdiff import DeepDiff

TestAcquisition = namedtuple('TestAcquisiton', ['data'])
TestDataset = namedtuple('TestDataset', ['shape', 'chunks'])

class TestCogtifOptions(TestClass):

    @classmethod
    def create_acq(cls, shape, chunks):
        return TestAcquisition(
            TestDataset(
                shape=shape,
                chunks=chunks
            )
        )

    def test_cogtif_low_res(self):
        low_res = self.create_acq((80, 80), (0,0))
        expected = {
            'options': {
                'compress': 'deflate',
                'zlevel': 4,
                'copy_src_overviews': 'yes'
            },
            'config_options': {}
        }

        results = get_cogtif_options(low_res)
        self.assertEqual(DeepDiff(results, expected), {})

    def test_cogtif_boundary_res(self):
        low_res = self.create_acq((1024, 512), (0,0))
        expected = {
            'options': {
                'compress': 'deflate',
                'zlevel': 4,
                'copy_src_overviews': 'yes'
                'blockysize': 80,
                'blockxsize': 256
            },
            'config_options': {}
        }

        results = get_cogtif_options(low_res)
        self.assertEqual(DeepDiff(results, expected), {})

    def test_cogtif_single_tile(self):
        low_res = self.create_acq((1024, 1024), (1024, 1024))
        expected = {
            'options': {
                'compress': 'deflate',
                'zlevel': 4,
                'copy_src_overviews': 'yes'
                'blockysize': 512,
                'blockxsize': 512,
            },
            'config_options': {
                'GDAL_TIFF_OVER_BLOCKSIZE': 512
            }
        }

        results = get_cogtif_options(low_res)
        self.assertEqual(DeepDiff(results, expected), {})

    def test_cogtif_tiled_res(self):
        low_res = self.create_acq((2048, 2048), (1024, 1024))
        expected = {
            'options': {
                'compress': 'deflate',
                'zlevel': 4,
                'copy_src_overviews': 'yes'
                'blockysize': 1024,
                'blockxsize': 1024,
            },
            'config_options': {}
        }

        results = get_cogtif_options(low_res)
        self.assertEqual(DeepDiff(results, expected), {})
