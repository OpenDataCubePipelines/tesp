# coding=utf-8
"""
Execution method for FMask - http://pythonfmask.org - (cloud, cloud shadow, water and
snow/ice classification) code supporting Sentinel-2 Level 1 C SAFE format zip archives hosted by the
Australian Copernicus Data Hub - http://www.copernicus.gov.au/ - for direct (zip) read access
by datacube.

example usage:
    fmask_cophub.py S2A_MSIL1C_20170104T052712_N0204_R019_T43MDR_20170104T052713.zip
    --output /tmp/
"""
from __future__ import absolute_import
import os
import tempfile
import logging
from xml.etree import ElementTree
from pathlib import Path
import zipfile
from collections import OrderedDict
import click
from pathlib import Path

from wagl.acquisition import acquisitions

os.environ["CPL_ZIP_ENCODING"] = "UTF-8"


def prepare_dataset(path, acq_parser_hint=None):
    """
    Returns a dictionary of image paths, granule id and metadata file location for the granules
    contained within the input file
    """

    acq_container = acquisitions(path, acq_parser_hint)
    tasks = []

    for granule_id in acq_container.granules:
        image_dict = OrderedDict([
            ('B01', {}), ('B02', {}), ('B03', {}), ('B04', {}), ('B05', {}),
            ('B06', {}), ('B07', {}), ('B08', {}), ('B8A', {}), ('B09', {}),
            ('B10', {}), ('B11', {}), ('B12', {})
        ])

        for group_id in acq_container.groups:
            acqs = acq_container.get_acquisitions(granule=granule_id,
                                                  group=group_id,
                                                  only_supported_bands=False)

            for acq in acqs:
                image_dict[Path(acq.uri).stem] = {'path': acq.uri, 'layer': '1'}

            tasks.append(tuple([image_dict, granule_id, acq.granule_xml]))

    return tasks


def fmask(dataset_path, task, out_fname, outdir):
    """
    Execute the fmask process.
    """
    dataset_path = Path(dataset_path)
    img_dict, granule_id, mtd_xml = task
    with tempfile.TemporaryDirectory(dir=outdir,
                                     prefix='pythonfmask-') as tmpdir:
        # filenames
        vrt_fname = os.path.join(tmpdir, granule_id + ".vrt")
        angles_fname = os.path.join(tmpdir, granule_id + ".angles.img")

        # zipfile extraction
        archive_container = os.path.join(tmpdir, Path(mtd_xml).name)
        if ".zip" in archive_container:
            logging.info("Unzipping "+mtd_xml)
            os.system("unzip -p " + str(dataset_path) + " " + mtd_xml + " > " + archive_container)

        # vrt creation
        command = ["gdalbuildvrt", "-resolution", "user", "-tr", "20", "20",
                   "-separate", "-overwrite", vrt_fname]
        for key in img_dict.keys():
            if ".zip" in archive_container:
                command.append(img_dict[key]['path'].replace('zip:', '/vsizip/').replace('!', "/"))
            else:
                command.append(img_dict[key]['path'])

        command_str = ' '.join(command)
        logging.info("Create  VRT " + vrt_fname)
        os.system(command_str)

        # angles generation
        if "zip" in archive_container:
            command = "fmask_sentinel2makeAnglesImage.py -i " + zipfile_path + " -o " + angles_fname
        else:
            command = "fmask_sentinel2makeAnglesImage.py -i " + mtd_xml + " -o " + angles_fname
        logging.info("Create angle file " + angles_fname)
        os.system(command)

        # run fmask
        command = "fmask_sentinel2Stacked.py -a " + vrt_fname + " -z " + angles_fname + " -o " + out_fname
        logging.info("Create fmask output " + out_fname)
        os.system(command)


def fmask_cogtif(fname, out_fname):
    """
    Convert the standard fmask output to a cloud optimised geotif.
    """
    command = ["gdal_translate",
               "-of",
               "GTiff",
               "-co",
               "COMPRESS=DEFLATE",
               "-co",
               "ZLEVEL=4",
               "-co",
               "PREDICTOR=2",
               "-co",
               "COPY_SRC_OVERVIEWS=YES",
               fname,
               out_fname]

    logging.info("Create fmask cogtif " + out_fname)
    os.system(' '.join(command))


@click.command(help=__doc__)
@click.option('--output', help="Write datasets into this directory",
              type=click.Path(exists=False, writable=True, dir_okay=True))
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)


def main(output, datasets):
    """
    For each dataset in input 'datasets' generate FMask and Contiguity
    outputs and write to the destination path specified by 'output'
    """
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
    for dataset in datasets:
        outpath = os.path.join(os.path.abspath(output),
                               os.path.basename(dataset) + '.fmask')
        if not os.path.exists(outpath):
            os.makedirs(outpath)

        path = Path(dataset)
        tasks = prepare_dataset(path)
        for i in tasks:
            out_fname = os.path.join(outpath, '{}.cloud.img'.format(i[1]))
            fmask(path, i, out_fname)


if __name__ == "__main__":
    main()
