#!/usr/bin/env python

import os
from os.path import join as pjoin, basename, dirname, splitext, exists
from subprocess import check_call
import tempfile
import glob
import argparse
import re
from pkg_resources import resource_stream
import numpy
import h5py
from rasterio.enums import Resampling

import yaml
from yaml.representer import Representer

from wagl.acquisition import acquisitions
from wagl.data import write_img
from wagl.hdf5 import find
from wagl.geobox import GriddedGeoBox

import tesp
from tesp.checksum import checksum
from tesp.contiguity import do_contiguity
from tesp.contrast import quicklook
from tesp.fmask_cophub import fmask_cogtif
from tesp.html_geojson import html_map
from tesp.yaml_merge import merge_metadata, image_dict

yaml.add_representer(numpy.int8, Representer.represent_int)
yaml.add_representer(numpy.uint8, Representer.represent_int)
yaml.add_representer(numpy.int16, Representer.represent_int)
yaml.add_representer(numpy.uint16, Representer.represent_int)
yaml.add_representer(numpy.int32, Representer.represent_int)
yaml.add_representer(numpy.uint32, Representer.represent_int)
yaml.add_representer(numpy.int, Representer.represent_int)
yaml.add_representer(numpy.int64, Representer.represent_int)
yaml.add_representer(numpy.uint64, Representer.represent_int)
yaml.add_representer(numpy.float, Representer.represent_float)
yaml.add_representer(numpy.float32, Representer.represent_float)
yaml.add_representer(numpy.float64, Representer.represent_float)
yaml.add_representer(numpy.ndarray, Representer.represent_list)

PRODUCTS = ['NBAR', 'NBART']
LEVELS = [2, 4, 8, 16, 32]
# TODO re-work so that pattern is not needed or caters for landsat
PATTERN = re.compile(
    r'(?P<prefix>(?:.*_)?)(?P<band_name>B[0-9][A0-9])'
    r'(?P<extension>\.TIF)')


def run_command(command, work_dir):
    """
    A simple utility to execute a subprocess command.
    """
    check_call(' '.join(command), shell=True, cwd=work_dir)


def wagl_unpack(scene, granule, h5group, outdir):
    """
    Unpack and package the NBAR and NBART products.
    """
    # listing of all datasets of IMAGE CLASS type
    img_paths = find(h5group, 'IMAGE')

    for product in PRODUCTS:
        for pathname in [p for p in img_paths if '/{}/'.format(product) in p]:

            dataset = h5group[pathname]
            if dataset.attrs['band_name'] == 'BAND-9':
                # TODO re-work so that a valid BAND-9 from another sensor isn't skipped
                continue

            acqs = scene.get_acquisitions(group=pathname.split('/')[0],
                                          granule=granule)
            acq = [a for a in acqs if
                   a.band_name == dataset.attrs['band_name']][0]

            # base_dir = pjoin(splitext(basename(acq.pathname))[0], granule)
            base_fname = '{}.TIF'.format(splitext(basename(acq.uri))[0])
            match_dict = PATTERN.match(base_fname).groupdict()
            fname = '{}{}_{}{}'.format(match_dict.get('prefix'), product,
                                       match_dict.get('band_name'),
                                       match_dict.get('extension'))
            out_fname = pjoin(outdir, product, fname.replace('L1C', 'ARD'))

            # output
            if not exists(dirname(out_fname)):
                os.makedirs(dirname(out_fname))

            write_img(dataset, out_fname, cogtif=True, levels=LEVELS,
                      nodata=dataset.attrs['no_data_value'],
                      geobox=GriddedGeoBox.from_dataset(dataset),
                      resampling=Resampling.nearest,
                      options={'blockxsize': dataset.chunks[1],
                               'blockysize': dataset.chunks[0],
                               'compress': 'deflate',
                               'zlevel': 4})

    # retrieve metadata
    scalar_paths = find(h5group, 'SCALAR')
    pathname = [pth for pth in scalar_paths if 'NBAR-METADATA' in pth][0]
    tags = yaml.load(h5group[pathname][()])
    return tags


# TODO re-work so that it is sensor independent
def build_vrts(outdir):
    """
    Build the various vrt's.
    """
    exe = 'gdalbuildvrt'

    for product in PRODUCTS:
        out_path = pjoin(outdir, product)
        expr = pjoin(out_path, '*_B02.TIF')
        base_name = basename(glob.glob(expr)[0]).replace('B02.TIF', '')

        out_fname = '{}ALLBANDS_20m.vrt'.format(base_name)
        cmd = [exe,
               '-resolution',
               'user',
               '-tr',
               '20',
               '20',
               '-separate',
               '-overwrite',
               out_fname,
               '*_B0[1-8].TIF',
               '*_B8A.TIF',
               '*_B1[1-2].TIF']
        run_command(cmd, out_path)

        out_fname = '{}10m.vrt'.format(base_name)
        cmd = [exe,
               '-separate',
               '-overwrite',
               out_fname,
               '*_B0[2-48].TIF']
        run_command(cmd, out_path)

        out_fname = '{}20m.vrt'.format(base_name)
        cmd = [exe,
               '-separate',
               '-overwrite',
               out_fname,
               '*_B0[5-7].TIF',
               '*_B8A.TIF',
               '*_B1[1-2].TIF']
        run_command(cmd, out_path)

        out_fname = '{}60m.vrt'.format(base_name)
        cmd = [exe,
               '-separate',
               '-overwrite',
               out_fname,
               '*_B01.TIF']
        run_command(cmd, out_path)


def create_contiguity(outdir):
    """
    Create the contiguity dataset for each
    """
    for product in PRODUCTS:
        out_path = pjoin(outdir, product)
        expr = pjoin(out_path, '*ALLBANDS_20m.vrt')
        fname = glob.glob(expr)[0]
        out_fname = fname.replace('.vrt', '_CONTIGUITY.TIF')

        # create contiguity
        do_contiguity(fname, out_fname)


def create_html_map(outdir):
    """
    Create the html map and GeoJSON valid data extents files.
    """
    expr = pjoin(outdir, 'NBAR', '*_CONTIGUITY.TIF')
    contiguity_fname = glob.glob(expr)[0]
    html_fname = pjoin(outdir, 'map.html')
    json_fname = pjoin(outdir, 'bounds.geojson')

    # html valid data extents
    html_map(contiguity_fname, html_fname, json_fname)


def create_quicklook(outdir):
    """
    Create the quicklook and thumbnail images.
    """
    for product in PRODUCTS:
        out_path = pjoin(outdir, product)
        fname = glob.glob(pjoin(out_path, '*10m.vrt'))[0]
        out_fname1 = fname.replace('10m.vrt', 'QUICKLOOK.TIF')
        out_fname2 = fname.replace('10m.vrt', 'THUMBNAIL.JPG')

        with tempfile.TemporaryDirectory(dir=out_path,
                                         prefix='quicklook-') as tmpdir:

            tmp_fname1 = pjoin(tmpdir, 'tmp1.tif')
            tmp_fname2 = pjoin(tmpdir, 'tmp2.tif')
            quicklook(fname, out_fname=tmp_fname1, src_min=1, src_max=3500,
                      out_min=1)

            # warp to Lon/Lat WGS84
            cmd = ['gdalwarp',
                   '-t_srs',
                   '"EPSG:4326"',
                   '-tap',
                   '-tap',
                   '-co',
                   'COMPRESS=JPEG',
                   '-co',
                   'PHOTOMETRIC=YCBCR',
                   '-co',
                   'TILED=YES',
                   '-tr',
                   '0.0001',
                   '0.0001',
                   tmp_fname1,
                   tmp_fname2]
            run_command(cmd, out_path)

            # build overviews/pyramids
            cmd = ['gdaladdo',
                   '-r',
                   'average',
                   tmp_fname2,
                   '2',
                   '4',
                   '8',
                   '16',
                   '32']
            run_command(cmd, out_path)

            # create the cogtif
            cmd = ['gdal_translate',
                   '-co',
                   'TILED=YES',
                   '-co',
                   'COPY_SRC_OVERVIEWS=YES',
                   '-co',
                   'COMPRESS=JPEG',
                   '-co',
                   'PHOTOMETRIC=YCBCR',
                   tmp_fname2,
                   out_fname1]
            run_command(cmd, out_path)

            # create the thumbnail
            cmd = ['gdal_translate',
                   '-of',
                   'JPEG',
                   '-outsize',
                   '10%',
                   '10%',
                   out_fname1,
                   out_fname2]
            run_command(cmd, out_path)


def create_readme(outdir):
    """
    Create the readme file.
    """
    with resource_stream(tesp.__name__, '_README') as src:
        with open(pjoin(outdir, 'README'), 'w') as out_src:
            out_src.writelines([l.decode('utf-8') for l in src.readlines()])


def create_checksum(outdir):
    """
    Create the checksum file.
    """
    out_fname = pjoin(outdir, 'CHECKSUM.sha1')
    checksum(out_fname)


def package(l1_path, wagl_fname, fmask_fname, yamls_path, outdir,
            s3_root, granule=None, acq_parser_hint=None):
    """
    Package an L2 product.

    :param l1_path:
        A string containing the full file pathname to the Level-1
        dataset.

    :param wagl_fname:
        A string containing the full file pathname to the wagl
        output dataset.

    :param fmask_fname:
        A string containing the full file pathname to the fmask
        dataset.

    :param yamls_path:
        A string containing the full file pathname to the yaml
        documents for the indexed Level-1 datasets.

    :param outdir:
        A string containing the full file pathname to the directory
        that will contain the packaged Level-2 datasets.

    :param acq_parser_hint:
        A string that hints at which acquisition parser should be used.

    :return:
        None; The packages will be written to disk directly.
    """
    scene = acquisitions(l1_path, acq_parser_hint)
    yaml_fname = pjoin(yamls_path,
                       basename(dirname(l1_path)),
                       '{}.yaml'.format(scene.label))
    with open(yaml_fname, 'r') as src:
        l1_documents = {doc['tile_id']: doc for doc in yaml.load_all(src)}

    with h5py.File(wagl_fname, 'r') as fid:
        grn_id = granule.replace('L1C', 'ARD')
        out_path = pjoin(outdir, grn_id)

        if not exists(out_path):
            os.makedirs(out_path)

        # fmask cogtif conversion
        fmask_cogtif(fmask_fname, pjoin(out_path, '{}_QA.TIF'.format(grn_id)))

        # unpack the data produced by wagl
        wagl_tags = wagl_unpack(scene, granule, fid[granule], out_path)

        # vrts, contiguity, map, quicklook, thumbnail, readme, checksum
        build_vrts(out_path)
        create_contiguity(out_path)
        create_html_map(out_path)
        create_quicklook(out_path)
        create_readme(out_path)

        # relative paths yaml doc
        # merge all the yaml documents
        tags = merge_metadata(l1_documents[granule], wagl_tags, out_path)

        with open(pjoin(out_path, 'ARD-METADATA.yaml'), 'w') as src:
            yaml.dump(tags, src, default_flow_style=False, indent=4)

        # create s3 paths for s3 yaml doc
        tags['image']['bands'] = image_dict(out_path, pjoin(s3_root, grn_id))

        with open(pjoin(out_path, 'ARD-METADATA-S3.yaml'), 'w') as src:
            yaml.dump(tags, src, default_flow_style=False, indent=4)

        # finally the checksum
        create_checksum(out_path)


if __name__ == '__main__':
    description = "Prepare or package a wagl output."
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument("--level1-pathname", required=True,
                        help="The level1 pathname.")
    parser.add_argument("--wagl-filename", required=True,
                        help="The filename of the wagl output.")
    parser.add_argument("--fmask-pathname", required=True,
                        help=("The pathname to the directory containing the "
                              "fmask results for the level1 dataset."))
    parser.add_argument("--prepare-yamls", required=True,
                        help="The pathname to the level1 prepare yamls.")
    parser.add_argument("--outdir", required=True,
                        help="The output directory.")

    args = parser.parse_args()

    package(args.level1_pathname, args.wagl_filename, args.fmask_pathname,
            args.prepare_yamls, args.outdir)
