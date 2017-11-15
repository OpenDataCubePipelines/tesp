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
import rasterio
import numpy as np
import click
os.environ["CPL_ZIP_ENCODING"] = "UTF-8"

def do_contiguity(fname, output):
    """
    Write a contiguity mask file based on the intersection of valid data pixels across all
    bands from the input file and output to the specified directory
    """
    bands = rasterio.open(fname)
    ones = np.ones((bands.height, bands.width), dtype='uint8')
    for band in bands.indexes:
        ones &= bands.read(band) > 0
    with rasterio.open(output, 'w', driver='HFA', width=bands.width, height=bands.height, \
                count=1, crs=bands.crs, transform=bands.transform, dtype='uint8') as outfile: outfile.write_band(1, ones)
    bands.close()
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
        contiguity = path+".contiguity.img"
        logging.info("Create contiguity image " + contiguity)
        do_contiguity(path, contiguity)

if __name__ == "__main__":
    main()
    