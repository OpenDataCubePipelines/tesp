# coding=utf-8
"""
Execution method for quicklook creatiob 

example usage:
    quicklook.py red.tif green.tif blue.tif --output quicklook.tif --scale 0 3500 0 255
    --output /tmp/
"""
from __future__ import absolute_import
import os
import logging
import rasterio
import numpy as np
from skimage import exposure, img_as_float
import click
from pathlib import Path

os.environ["CPL_ZIP_ENCODING"] = "UTF-8"


def make_quicklook(datasets, src_min, src_max, null, output):
    
    r = rasterio.open(datasets[0])
    g = rasterio.open(datasets[1])
    b = rasterio.open(datasets[2])

    r_rescale = exposure.rescale_intensity(r, in_range=(src_min, src_max))
    g_rescale = exposure.rescale_intensity(g, in_range=(src_min, src_max))
    b_rescale = exposure.rescale_intensity(b, in_range=(src_min, src_max))

    rgb_stack = np.dstack((r_rescale,g_rescale,b_rescale))
    img = img_as_float(rgb_stack)

    with rasterio.open(output, 'w', driver='GTiff', width=bands.width, height=bands.height, \
                count=1, crs=bands.crs, transform=bands.transform, dtype='uint8') as outfile: outfile.write_band(1, ones)



@click.command(help=__doc__)
@click.option('--output', help="output dataset",
              type=click.Path(exists=False, writable=True, dir_okay=True))
@click.argument('datasets', help="<red.tif> <green.tif> <red.tif>",
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@click.argument('scale', help="<src min> <src max> <dst min> <dst max>", nargs=4, type-float)

def main(output, datasets):
    """
    For input colour bands generate full resolution quickook
    output in 8/24 bit colour and write to the destination path specified by 'output' 
    """
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
    print(type(datasets))
    for dataset in datasets:
        path = dataset
        os.path.commonprefix(datasets)
        stem = Path(path).stem
        out = os.path.join(output, stem)
        quicklook = out+".QUICKLOOK.img"
        logging.info("Create quicklook " + contiguity)
        do_contiguity(path, contiguity)

if __name__ == "__main__":
    main()
    