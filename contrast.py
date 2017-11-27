#!/usr/bin/env python

import argparse
import numpy
import rasterio
from rasterio.enums import Resampling
from skimage.exposure import rescale_intensity

FACTORS = [2, 4, 8, 16, 32]


def quicklook(fname, out_fname, src_min, src_max, out_min=0, out_max=255):
    """
    Generate a quicklook image applying a linear contrast enhancement.
    Outputs will be converted to Uint8.
    If the input image has a valid no data value, the no data will
    be set to 0 in the output image.
    Any non-contiguous data across the colour domain, will be set to
    zero.

    :param fname:
        A `str` containing the file pathname to an image containing
        the relevant data to extract.

    :param out_fname:
        A `str` containing the file pathname to where the quicklook
        image will be saved.

    :param src_min:
        An integer/float containing the minimum data value to be
        used as input into contrast enhancement.

    :param src_max:
        An integer/float containing the maximum data value to be
        used as input into the contrast enhancement.

    :param out_min:
        An integer specifying the minimum output value that `src_min`
        will be scaled to. Default is 0.

    :param out_max:
        An integer specifying the maximum output value that `src_max`
        will be scaled to. Default is 255.

    :return:
        None; The output will be written directly to disk.
        The output datatype will be `UInt8`.
    """
    bands = [4, 3, 2]
    with rasterio.open(fname) as ds:

        # no data locations
        nulls = numpy.zeros((ds.height, ds.width), dtype='bool')
        for band in bands:
            data = ds.read(band)

            nulls &= data == ds.nodata

        kwargs = {'driver': "GTiff",
                  'height': ds.height,
                  'width': ds.width,
                  'count': 3,
                  'dtype': 'uint8',
                  'crs': ds.crs,
                  'transform': ds.transform,
                  'nodata': 0,
                  'compress': 'jpeg',
                  'photometric': 'YCBCR',
                  'tiled': 'yes',
                  'blockxsize': 512,
                  'blockysize': 512}

        with rasterio.open(out_fname, 'w', **kwargs) as out_ds:
            for i, band in enumerate(bands):
                data = ds.read(band)
                scaled = rescale_intensity(data, in_range=(src_min, src_max),
                                           out_range=(out_min, out_max))
                scaled[nulls] = 0

                out_ds.write(scaled.astype('uint8'), i + 1)

            out_ds.build_overviews(FACTORS, Resampling.average)


if __name__ == '__main__':
    description = "Quicklook generation."
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument("--filename", required=True,
                        help="The input filename.")
    parser.add_argument("--out_fname", required=True,
                        help="The output filename.")
    parser.add_argument("--src_min", type=int, required=True,
                        help="Source minimum")
    parser.add_argument("--src_max", type=int, required=True,
                        help="Source maximum")
    parser.add_argument("--out_min", type=int, default=0,
                        help="Output minimum")
    parser.add_argument("--out_max", type=int, default=255,
                        help="Output maximum")

    args = parser.parse_args()
    quicklook(args.filename, args.out_fname, args.src_min, args.src_max,
              args.out_min, args.out_max)
