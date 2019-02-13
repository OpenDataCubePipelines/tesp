from unittest import TestCase
from collections import namedtuple

from tesp.package import get_cogtif_options
from deepdiff import DeepDiff

TempDataset = namedtuple('TempDataset', ['shape', 'chunks'])


class TestCogtifOptions(TestCase):

    @classmethod
    def create_acq(cls, shape, chunks):
        return TempDataset(
            shape=shape,
            chunks=chunks
        )

    def test_cogtif_low_res(self):
        low_res = self.create_acq((80, 80), (0, 0))
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
        low_res = self.create_acq((1024, 512), (512, 512))
        expected = {
            'options': {
                'compress': 'deflate',
                'zlevel': 4,
                'copy_src_overviews': 'yes',
                'blockysize': 512,
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
                'blockysize': 512,
                'blockxsize': 512,
                'tiled': 'yes',
                'copy_src_overviews': 'yes'
            },
            'config_options': {
                'GDAL_TIFF_OVR_BLOCKSIZE': 512
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
                'copy_src_overviews': 'yes',
                'blockysize': 1024,
                'blockxsize': 1024,
                'tiled': 'yes'
            },
            'config_options': {}
        }

        results = get_cogtif_options(low_res)
        self.assertEqual(DeepDiff(results, expected), {})
