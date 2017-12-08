# coding=utf-8
"""
Execution method for contiguous observations within band stack

example usage:
    contiguity.py <allbands.vrt>
    --output /tmp/
"""
from __future__ import absolute_import
import os
import logging
from pathlib import Path
import rasterio
import numpy as np
import click
from gaip.data import write_img
from gaip.geobox import GriddedGeoBox

os.environ["CPL_ZIP_ENCODING"] = "UTF-8"

def do_contiguity(fname, output):
    """
    Write a contiguity mask file based on the intersection of valid data pixels across all
    bands from the input file and output to the specified directory
    """
    with rasterio.open(fname) as ds:
        geobox = GriddedGeoBox.from_dataset(ds)
        yblock, xblock = ds.block_shapes[0]
        ones = np.ones((ds.height, ds.width), dtype='uint8')
        for band in ds.indexes:
            ones &= ds.read(band) > 0

    co_options = {'compress': 'deflate',
                  'zelevel': 4,
                  'blockxsize': xblock,
                  'blockysize': yblock}
    write_img(ones, output, cogtif=True, levels=[2, 4, 8, 16, 32],
              geobox=geobox, options=co_options)

    return None

@click.command(help=__doc__)
@click.option('--output', help="Write contiguity datasets into this directory",
              type=click.Path(exists=False, writable=True, dir_okay=True))
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)

def main(output, datasets):
    """
    For input 'vrt' generate Contiguity
    outputs and write to the destination path specified by 'output'
    """
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
    for dataset in datasets:
        path = dataset
        stem = Path(path).stem
        out = os.path.join(output, stem)
        contiguity = out+".contiguity.img"
        logging.info("Create contiguity image " + contiguity)
        do_contiguity(path, contiguity)

if __name__ == "__main__":
    main()
