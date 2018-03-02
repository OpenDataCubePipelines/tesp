#!/usr/bin/env python

import os
from os.path import join as pjoin, basename, dirname, splitext, exists
from posixpath import join as ppjoin
from pathlib import Path
from subprocess import check_call
import tempfile
import glob
import re
from pkg_resources import resource_stream
import numpy
import h5py
from rasterio.enums import Resampling

import yaml
from yaml.representer import Representer

from wagl.acquisition import acquisitions
from wagl.constants import DatasetName, GroupName
from wagl.data import write_img
from wagl.hdf5 import find
from wagl.geobox import GriddedGeoBox

import tesp
from tesp.checksum import checksum
from tesp.contrast import quicklook
from tesp.html_geojson import html_map
from tesp.yaml_merge import merge_metadata

from eugl.fmask import fmask_cogtif
from eugl.contiguity import contiguity

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
ALIAS_FMT = {'NBAR': '{}', 'NBART': 't_{}'}
LEVELS = [2, 4, 8, 16, 32]
PATTERN1 = re.compile(
    r'(?P<prefix>(?:.*_)?)(?P<band_name>B[0-9][A0-9]|B[0-9]*|B[0-9a-zA-z]*)'
    r'(?P<extension>\.TIF)')
PATTERN2 = re.compile('(L1[GTPC]{1,2})')
ARD = 'ARD'
QA = 'QA'
SUPPS = 'SUPPLEMENTARY'


def run_command(command, work_dir):
    """
    A simple utility to execute a subprocess command.
    """
    check_call(' '.join(command), shell=True, cwd=work_dir)


def _write_cogtif(dataset, out_fname):
    """
    Easy wrapper for writing a cogtif, that takes care of datasets
    that are written row by row rather square(ish) blocks.
    """
    if dataset.chunks[1] == dataset.shape[1]:
        blockxsize = 512
        blockysize = 512
        data = dataset[:]
    else:
        blockysize, blockxsize = dataset.chunks
        data = dataset

    options = {'blockxsize': blockxsize,
               'blockysize': blockysize,
               'compress': 'deflate',
               'zlevel': 4}

    nodata = dataset.attrs.get('no_data_value')
    geobox = GriddedGeoBox.from_dataset(dataset)

    # path existence
    if not exists(dirname(out_fname)):
        os.makedirs(dirname(out_fname))

    write_img(data, out_fname, cogtif=True, levels=LEVELS, nodata=nodata,
              geobox=geobox, resampling=Resampling.nearest, options=options)


def unpack_products(container, granule, h5group, outdir):
    """
    Unpack and package the NBAR and NBART products.
    """
    # listing of all datasets of IMAGE CLASS type
    img_paths = find(h5group, 'IMAGE')

    # relative paths of each dataset for ODC metadata doc
    rel_paths = {}

    # TODO pass products through from the scheduler rather than hard code
    for product in PRODUCTS:
        for pathname in [p for p in img_paths if '/{}/'.format(product) in p]:

            dataset = h5group[pathname]

            acqs = container.get_acquisitions(group=pathname.split('/')[0],
                                              granule=granule)
            acq = [a for a in acqs if
                   a.band_name == dataset.attrs['band_name']][0]

            base_fname = '{}.TIF'.format(splitext(basename(acq.uri))[0])
            match_dict = PATTERN1.match(base_fname).groupdict()
            fname = '{}{}_{}{}'.format(match_dict.get('prefix'), product,
                                       match_dict.get('band_name'),
                                       match_dict.get('extension'))
            rel_path = pjoin(product, re.sub(PATTERN2, ARD, fname))
            out_fname = pjoin(outdir, rel_path)

            _write_cogtif(dataset, out_fname)

            # alias name for ODC metadata doc
            alias = ALIAS_FMT[product].format(dataset.attrs['alias'])
            rel_paths[alias.lower()] = {'path': rel_path, 'layer': 1}

    # retrieve metadata
    scalar_paths = find(h5group, 'SCALAR')
    pathname = [pth for pth in scalar_paths if 'NBAR-METADATA' in pth][0]
    tags = yaml.load(h5group[pathname][()])
    return tags, rel_paths


def unpack_supplementary(container, granule, h5group, outdir):
    """
    Unpack the angles + other supplementary datasets produced by wagl.
    Currently only the mode resolution group gets extracted.
    """
    _, res_grp = container.get_mode_resolution(granule)
    grn_id = re.sub(PATTERN2, ARD, granule)
    fmt = '{}_{}.TIF'

    # relative paths of each dataset for ODC metadata doc
    rel_paths = {}

    # satellite and solar angles
    grp = h5group[ppjoin(res_grp, GroupName.sat_sol_group.value)]
    dnames = [DatasetName.satellite_view.value,
              DatasetName.satellite_azimuth.value,
              DatasetName.solar_zenith.value,
              DatasetName.solar_azimuth.value,
              DatasetName.relative_azimuth.value,
              DatasetName.time.value]

    for dname in dnames:
        rel_path = pjoin(SUPPS, fmt.format(grn_id, dname))
        out_fname = pjoin(outdir, rel_path)
        dset = grp[dname]
        rel_paths[dset.attrs['alias'].lower()] = {'path': rel_path, 'layer': 1}
        _write_cogtif(dset, out_fname.replace('-', '_'))

    # incident angles
    grp = h5group[ppjoin(res_grp, GroupName.incident_group.value)]
    dnames = [DatasetName.incident.value,
              DatasetName.azimuthal_incident.value]

    for dname in dnames:
        rel_path = pjoin(SUPPS, fmt.format(grn_id, dname))
        out_fname = pjoin(outdir, rel_path)
        dset = grp[dname]
        rel_paths[dset.attrs['alias'].lower()] = {'path': rel_path, 'layer': 1}
        _write_cogtif(dset, out_fname.replace('-', '_'))

    # exiting angles
    grp = h5group[ppjoin(res_grp, GroupName.exiting_group.value)]
    dnames = [DatasetName.exiting.value,
              DatasetName.azimuthal_exiting.value]

    for dname in dnames:
        rel_path = pjoin(SUPPS, fmt.format(grn_id, dname))
        out_fname = pjoin(outdir, rel_path)
        dset = grp[dname]
        rel_paths[dset.attrs['alias'].lower()] = {'path': rel_path, 'layer': 1}
        _write_cogtif(dset, out_fname.replace('-', '_'))

    # relative slope
    grp = h5group[ppjoin(res_grp, GroupName.rel_slp_group.value)]
    dnames = [DatasetName.relative_slope.value]

    for dname in dnames:
        rel_path = pjoin(SUPPS, fmt.format(grn_id, dname))
        out_fname = pjoin(outdir, rel_path)
        dset = grp[dname]
        rel_paths[dset.attrs['alias'].lower()] = {'path': rel_path, 'layer': 1}
        _write_cogtif(dset, out_fname.replace('-', '_'))

    # terrain shadow
    grp = h5group[ppjoin(res_grp, GroupName.shadow_group.value)]
    dnames = [DatasetName.combined_shadow.value]

    for dname in dnames:
        rel_path = pjoin(QA, fmt.format(grn_id, dname))
        out_fname = pjoin(outdir, rel_path)
        dset = grp[dname]
        rel_paths[dset.attrs['alias'].lower()] = {'path': rel_path, 'layer': 1}
        _write_cogtif(dset, out_fname.replace('-', '_'))

    # TODO do we also include slope and aspect?

    return rel_paths


def create_contiguity(container, granule, outdir):
    """
    Create the contiguity (all pixels valid) dataset.
    """
    # quick decision to use the mode resolution to form contiguity
    # this rule is expected to change once more people get involved
    # in the decision making process
    acqs, _ = container.get_mode_resolution(granule)
    grn_id = re.sub(PATTERN2, ARD, granule)

    # relative paths of each dataset for ODC metadata doc
    rel_paths = {}

    with tempfile.TemporaryDirectory(dir=outdir,
                                     prefix='contiguity-') as tmpdir:
        for product in PRODUCTS:
            search_path = pjoin(outdir, product)
            fnames = [str(f) for f in Path(search_path).glob('*.TIF')]

            # output filename
            base_fname = '{}_{}_CONTIGUITY.TIF'.format(grn_id, product)
            rel_path = pjoin(QA, base_fname)
            out_fname = pjoin(outdir, rel_path)

            alias = ALIAS_FMT[product].format('contiguity')
            rel_paths[alias] = {'path': rel_path, 'layer': 1}

            # temp vrt
            tmp_fname = pjoin(tmpdir, '{}.vrt'.format(product))
            cmd = ['gdalbuildvrt'
                   '-resolution',
                   'user',
                   '-tr',
                   str(acqs[0].resolution[1]),
                   str(acqs[0].resolution[0]),
                   '-separate',
                   tmp_fname]
            cmd.extend(fnames)
            run_command(cmd, tmpdir)

            # create contiguity
            contiguity(tmp_fname, out_fname)

    return rel_paths


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


def create_quicklook(container, outdir):
    """
    Create the quicklook and thumbnail images.
    """
    acq = container.get_acquisitions()[0]

    # are quicklooks still needed?
    # this wildcard mechanism needs to change if quicklooks are to
    # persist
    band_wcards = {'LANDSAT_5': 'L*_B[4,3,2].TIF',
                   'LANDSAT_7': 'L*_B[4,3,2].TIF',
                   'LANDSAT_8': 'L*_B[5,4,3].TIF',
                   'SENTINEL_2A': '*_B0[8,4,3].TIF',
                   'SENTINEL_2B': '*_B0[8,4,3].TIF'}

    # appropriate wildcard
    wcard = band_wcards[acq.platform_id]

    with tempfile.TemporaryDirectory(dir=outdir,
                                     prefix='quicklook-') as tmpdir:

        for product in PRODUCTS:
            out_path = Path(pjoin(outdir, product))
            fnames = [str(f) for f in out_path.glob(wcard)]

            # output filenames
            match = PATTERN1.match(fnames[0]).groupdict()
            out_fname1 = '{}{}{}'.format(match.get('prefix'),
                                         'QUICKLOOK',
                                         match.get('extension'))
            out_fname2 = '{}{}{}'.format(match.get('prefix'),
                                         'THUMBNAIL',
                                         '.JPG')

            # initial vrt of required rgb bands
            tmp_fname1 = pjoin(tmpdir, '{}.vrt'.format(product))
            cmd = ['gdalbuildvrt',
                   '-separate',
                   tmp_fname1]
            cmd.extend(fnames)
            run_command(cmd, tmpdir)

            # quicklook with contrast scaling
            tmp_fname2 = pjoin(tmpdir, '{}_{}.tif'.format(product, 'qlook'))
            quicklook(tmp_fname1, out_fname=tmp_fname2, src_min=1,
                      src_max=3500, out_min=1)

            # warp to Lon/Lat WGS84
            tmp_fname3 = pjoin(tmpdir, '{}_{}.tif'.format(product, 'warp'))
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
                   tmp_fname2,
                   tmp_fname3]
            run_command(cmd, tmpdir)

            # build overviews/pyramids
            cmd = ['gdaladdo',
                   '-r',
                   'average',
                   tmp_fname3,
                   '2',
                   '4',
                   '8',
                   '16',
                   '32']
            run_command(cmd, tmpdir)

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
                   tmp_fname3,
                   out_fname1]
            run_command(cmd, tmpdir)

            # create the thumbnail
            cmd = ['gdal_translate',
                   '-of',
                   'JPEG',
                   '-outsize',
                   '10%',
                   '10%',
                   out_fname1,
                   out_fname2]
            run_command(cmd, tmpdir)


def create_readme(outdir):
    """
    Create the readme file.
    """
    with resource_stream(tesp.__name__, '_README') as src:
        with open(pjoin(outdir, 'README.md'), 'w') as out_src:
            out_src.writelines([l.decode('utf-8') for l in src.readlines()])


def create_checksum(outdir):
    """
    Create the checksum file.
    """
    out_fname = pjoin(outdir, 'CHECKSUM.sha1')
    checksum(out_fname)


def package(l1_path, wagl_fname, fmask_fname, yamls_path, outdir,
            url_root, granule=None, acq_parser_hint=None):
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

    :param url_root:
        A string containing the url root path of a http address such as
        an Amazon S3 or Google Cloud Storage bucket.

    :param acq_parser_hint:
        A string that hints at which acquisition parser should be used.

    :return:
        None; The packages will be written to disk directly.
    """
    container = acquisitions(l1_path, acq_parser_hint)
    yaml_fname = pjoin(yamls_path,
                       basename(dirname(l1_path)),
                       '{}.yaml'.format(container.label))
    with open(yaml_fname, 'r') as src:
        l1_documents = {doc['tile_id']: doc for doc in yaml.load_all(src)}

    with h5py.File(wagl_fname, 'r') as fid:
        grn_id = re.sub(PATTERN2, ARD, granule)
        out_path = pjoin(outdir, grn_id)

        if not exists(out_path):
            os.makedirs(out_path)

        # unpack the standardised products produced by wagl
        wagl_tags, img_paths = unpack_products(container, granule,
                                               fid[granule], out_path)

        # unpack supplementary datasets produced by wagl
        supp_paths = unpack_supplementary(container, granule, fid[granule],
                                          out_path)

        # add in supp paths
        for key in supp_paths:
            img_paths[key] = supp_paths[key]

        # file based globbing, so can't have any other tifs on disk
        qa_paths = create_contiguity(container, granule, out_path)

        # add in qa paths
        for key in qa_paths:
            img_paths[key] = qa_paths[key]

        # fmask cogtif conversion
        rel_path = pjoin(QA, '{}_FMASK.TIF'.format(grn_id))
        img_paths['pixel_quality'] = {'path': rel_path, 'layer': 1}
        fmask_cogtif(fmask_fname, pjoin(out_path, rel_path))

        # map, quicklook/thumbnail, readme, checksum
        create_html_map(out_path)
        create_quicklook(container, out_path)
        create_readme(out_path)

        # merge all the yaml documents
        # TODO include gqa yaml, and fmask yaml (if we go ahead and create one)
        # relative paths yaml doc
        tags = merge_metadata(l1_documents[granule], wagl_tags, out_path)

        with open(pjoin(out_path, 'ARD-METADATA.yaml'), 'w') as src:
            yaml.dump(tags, src, default_flow_style=False, indent=4)

        # update to url/http paths
        url_paths = img_paths.copy()
        for key in url_paths:
            url_paths[key]['path'] = pjoin(url_root, url_paths[key]['path'])

        # create http paths for s3 yaml doc
        tags['image']['bands'] = url_paths

        with open(pjoin(out_path, 'ARD-METADATA-S3.yaml'), 'w') as src:
            yaml.dump(tags, src, default_flow_style=False, indent=4)

        # finally the checksum
        create_checksum(out_path)
