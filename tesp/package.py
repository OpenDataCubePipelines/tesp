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
import rasterio

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
from tesp.constants import ProductPackage
from tesp.ga_metadata import extract_level1_metadata

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

ALIAS_FMT = {'LAMBERTIAN': 'lambertian_{}', 'NBAR': 'nbar_{}', 'NBART': 'nbart_{}', 'SBT': 'sbt_{}'}
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


def _clean(alias):
    """
    A quick fix for cleaning json unfriendly alias strings.
    """
    replace = {'-': '_',
               '[': '',
               ']': ''}
    for k, v in replace.items():
        alias = alias.replace(k, v)

    return alias.lower()


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

def get_img_dataset_info(dataset, path, layer=1):
    """
    Returns metadata for raster datasets
    """
    geobox = GriddedGeoBox.from_dataset(dataset)
    return {
        'path': path,
        'layer': layer,
        'info': {
            'width': geobox.x_size(),
            'height': geobox.y_size(),
            'geotransform': list(geobox.transform.to_gdal())
        }
    }


def unpack_products(product_list, container, granule, h5group, outdir):
    """
    Unpack and package the NBAR and NBART products.
    """
    # listing of all datasets of IMAGE CLASS type
    img_paths = find(h5group, 'IMAGE')

    # relative paths of each dataset for ODC metadata doc
    rel_paths = {}

    # TODO pass products through from the scheduler rather than hard code
    for product in product_list:
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
            alias = _clean(ALIAS_FMT[product].format(dataset.attrs['alias']))

            # Band Metadata
            rel_paths[alias] = get_img_dataset_info(dataset, rel_path)

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
    def _write(dataset_names, h5_group, granule_id, basedir):
        """
        An internal util for serialising the supplementary
        H5Datasets to cogtif.
        """
        fmt = '{}_{}.TIF'
        paths = {}
        for dname in dataset_names:
            rel_path = pjoin(basedir,
                             fmt.format(granule_id, dname.replace('-', '_')))
            out_fname = pjoin(outdir, rel_path)
            dset = h5_group[dname]
            alias = _clean(dset.attrs['alias'])
            paths[alias] = get_img_dataset_info(dset, rel_path)
            _write_cogtif(dset, out_fname)

        return paths

    _, res_grp = container.get_mode_resolution(granule)
    grn_id = re.sub(PATTERN2, ARD, granule)

    # relative paths of each dataset for ODC metadata doc
    rel_paths = {}

    # satellite and solar angles
    grp = h5group[ppjoin(res_grp, GroupName.SAT_SOL_GROUP.value)]
    dnames = [DatasetName.SATELLITE_VIEW.value,
              DatasetName.SATELLITE_AZIMUTH.value,
              DatasetName.SOLAR_ZENITH.value,
              DatasetName.SOLAR_AZIMUTH.value,
              DatasetName.RELATIVE_AZIMUTH.value,
              DatasetName.TIME.value]
    paths = _write(dnames, grp, grn_id, SUPPS)
    for key in paths:
        rel_paths[key] = paths[key]

    # incident angles
    grp = h5group[ppjoin(res_grp, GroupName.INCIDENT_GROUP.value)]
    dnames = [DatasetName.INCIDENT.value,
              DatasetName.AZIMUTHAL_INCIDENT.value]
    paths = _write(dnames, grp, grn_id, SUPPS)
    for key in paths:
        rel_paths[key] = paths[key]

    # exiting angles
    grp = h5group[ppjoin(res_grp, GroupName.EXITING_GROUP.value)]
    dnames = [DatasetName.EXITING.value,
              DatasetName.AZIMUTHAL_EXITING.value]
    paths = _write(dnames, grp, grn_id, SUPPS)
    for key in paths:
        rel_paths[key] = paths[key]

    # relative slope
    grp = h5group[ppjoin(res_grp, GroupName.REL_SLP_GROUP.value)]
    dnames = [DatasetName.RELATIVE_SLOPE.value]
    paths = _write(dnames, grp, grn_id, SUPPS)
    for key in paths:
        rel_paths[key] = paths[key]

    # terrain shadow
    grp = h5group[ppjoin(res_grp, GroupName.SHADOW_GROUP.value)]
    dnames = [DatasetName.COMBINED_SHADOW.value]
    paths = _write(dnames, grp, grn_id, QA)
    for key in paths:
        rel_paths[key] = paths[key]

    # TODO do we also include slope and aspect?

    return rel_paths


def create_contiguity(product_list, container, granule, outdir):
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
        for product in product_list:
            search_path = pjoin(outdir, product)
            fnames = [str(f) for f in Path(search_path).glob('*.TIF')]

            # quick work around for products that aren't being packaged
            if not fnames:
                continue

            # output filename
            base_fname = '{}_{}_CONTIGUITY.TIF'.format(grn_id, product)
            rel_path = pjoin(QA, base_fname)
            out_fname = pjoin(outdir, rel_path)

            if not exists(dirname(out_fname)):
                os.makedirs(dirname(out_fname))

            alias = ALIAS_FMT[product].format('contiguity')

            # temp vrt
            tmp_fname = pjoin(tmpdir, '{}.vrt'.format(product))
            cmd = ['gdalbuildvrt',
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
            with rasterio.open(out_fname) as ds:
                rel_paths[alias] = get_img_dataset_info(ds, rel_path)

    return rel_paths


def create_html_map(outdir):
    """
    Create the html map and GeoJSON valid data extents files.
    """
    expr = pjoin(outdir, QA, '*_FMASK.TIF')
    contiguity_fname = glob.glob(expr)[0]
    html_fname = pjoin(outdir, 'map.html')
    json_fname = pjoin(outdir, 'bounds.geojson')

    # html valid data extents
    html_map(contiguity_fname, html_fname, json_fname)


def create_quicklook(product_list, container, outdir):
    """
    Create the quicklook and thumbnail images.
    """
    acq = container.get_acquisitions(None, None, False)[0]

    # are quicklooks still needed?
    # this wildcard mechanism needs to change if quicklooks are to
    # persist
    band_wcards = {'LANDSAT_5': ['L*_B{}.TIF'.format(i) for i in [3, 2, 1]],
                   'LANDSAT_7': ['L*_B{}.TIF'.format(i) for i in [3, 2, 1]],
                   'LANDSAT_8': ['L*_B{}.TIF'.format(i) for i in [4, 3, 2]],
                   'SENTINEL_2A': ['*_B0{}.TIF'.format(i) for i in [4, 3, 2]],
                   'SENTINEL_2B': ['*_B0{}.TIF'.format(i) for i in [4, 3, 2]]}

    # appropriate wildcards
    wcards = band_wcards[acq.platform_id]

    with tempfile.TemporaryDirectory(dir=outdir,
                                     prefix='quicklook-') as tmpdir:

        for product in product_list:
            if product == 'SBT':
                # no sbt quicklook for the time being
                continue

            out_path = Path(pjoin(outdir, product))
            fnames = []
            for wcard in wcards:
                fnames.extend([str(f) for f in out_path.glob(wcard)])

            # quick work around for products that aren't being packaged
            if not fnames:
                continue

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
                   '-overwrite',
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
    with resource_stream(tesp.__name__, '_README.md') as src:
        with open(pjoin(outdir, 'README.md'), 'w') as out_src:
            out_src.writelines([l.decode('utf-8') for l in src.readlines()])


def create_checksum(outdir):
    """
    Create the checksum file.
    """
    out_fname = pjoin(outdir, 'CHECKSUM.sha1')
    checksum(out_fname)


def get_level1_tags(container, granule=None, yamls_path=None, l1_path=None):
    if yamls_path:
        # TODO define a consistent file structure where yaml metadata exists
        yaml_fname = pjoin(yamls_path,
                           basename(dirname(l1_path)),
                           '{}.yaml'.format(container.label))

        # quick workaround if no source yaml
        if not exists(yaml_fname):
            raise IOError('yaml file not found: {}'.format(yaml_fname))

        with open(yaml_fname, 'r') as src:
            # TODO harmonise field names for different sensors
            l1_documents = {
                doc.get('tile_id', doc.get('label')): doc
                for doc in yaml.load_all(src)
            }
            l1_tags = l1_documents[granule]
    else:
        acq = container.get_all_acquisitions()[0]
        docs = extract_level1_metadata(acq, l1_path)
        if granule is not None and isinstance(docs, list):
            l1_tags = [doc for doc in docs
                       if doc.get('tile_id', doc.get('label')) == granule][0]
        else:
            l1_tags = docs
    return l1_tags


def package(l1_path, wagl_fname, fmask_fname, gqa_fname, yamls_path, outdir,
            granule, products=ProductPackage.all(), acq_parser_hint=None):
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

    :param gqa_fname:
        A string containing the full file pathname to the GQA yaml.

    :param yamls_path:
        A string containing the full file pathname to the yaml
        documents for the indexed Level-1 datasets.

    :param outdir:
        A string containing the full file pathname to the directory
        that will contain the packaged Level-2 datasets.

    :param granule:
        The identifier for the granule

    :param products:
        A list of imagery products to include in the package.
        Defaults to all products.

    :param acq_parser_hint:
        A string that hints at which acquisition parser should be used.

    :return:
        None; The packages will be written to disk directly.
    """
    container = acquisitions(l1_path, acq_parser_hint)
    l1_tags = get_level1_tags(container, granule, yamls_path, l1_path)


    with h5py.File(wagl_fname, 'r') as fid:
        grn_id = re.sub(PATTERN2, ARD, granule)
        out_path = pjoin(outdir, grn_id)

        if not exists(out_path):
            os.makedirs(out_path)

        # unpack the standardised products produced by wagl
        wagl_tags, img_paths = unpack_products(products, container, granule,
                                               fid[granule], out_path)

        # unpack supplementary datasets produced by wagl
        supp_paths = unpack_supplementary(container, granule, fid[granule],
                                          out_path)

        # add in supplementary paths
        for key in supp_paths:
            img_paths[key] = supp_paths[key]

        # file based globbing, so can't have any other tifs on disk
        qa_paths = create_contiguity(products, container, granule, out_path)

        # add in qa paths
        for key in qa_paths:
            img_paths[key] = qa_paths[key]

        # fmask cogtif conversion
        rel_path = pjoin(QA, '{}_FMASK.TIF'.format(grn_id))
        fmask_location = pjoin(out_path, rel_path)
        fmask_cogtif(fmask_fname, fmask_location)

        with rasterio.open(fmask_location) as ds:
            img_paths['fmask'] = get_img_dataset_info(ds, rel_path)

        # map, quicklook/thumbnail, readme, checksum
        create_html_map(out_path)
        create_quicklook(products, container, out_path)
        create_readme(out_path)

        # merge all the yaml documents
        # TODO include fmask yaml (if we go ahead and create one)
        # TODO record gverify version
        # TODO put eugl, fmask, tesp in the software_versions section
        # relative paths yaml doc
        with open(gqa_fname) as fl:
            gqa_tags = yaml.load(fl)

        tags = merge_metadata(l1_tags, wagl_tags, gqa_tags, granule, img_paths)

        with open(pjoin(out_path, 'ARD-METADATA.yaml'), 'w') as src:
            yaml.dump(tags, src, default_flow_style=False, indent=4)

        # finally the checksum
        create_checksum(out_path)
