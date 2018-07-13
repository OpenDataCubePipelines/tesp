"""
Ingest data from the command-line.
"""
from __future__ import absolute_import

import uuid
import logging
import yaml
import re
from urllib.request import urlopen
from urllib.parse import urlparse, urljoin
from datetime import datetime
from os.path import join as pjoin
import hashlib

import click
from click_datetime import Datetime
from osgeo import osr
import os
from pathlib import Path

LANDSAT_8_BANDS = [
    ('1', 'coastal_aerosol'),
    ('2', 'blue'),
    ('3', 'green'),
    ('4', 'red'),
    ('5', 'nir'),
    ('6', 'swir1'),
    ('7', 'swir2'),
    ('8', 'panchromatic'),
    ('9', 'cirrus'),
    ('10', 'lwir1'),
    ('11', 'lwir2'),
    ('QUALITY', 'quality')]

TIRS_ONLY = [
    ('10', 'lwir1'),
    ('11', 'lwir2'),
    ('QUALITY', 'quality')]


LANDSAT_BANDS = [
    ('1', 'blue'),
    ('2', 'green'),
    ('3', 'red'),
    ('4', 'nir'),
    ('5', 'swir1'),
    ('7', 'swir2'),
    ('QUALITY', 'quality')]


MTL_PAIRS_RE = re.compile(r'(\w+)\s=\s(.*)')


def _parse_value(s):
    s = s.strip('"')
    for parser in [int, float]:
        try:
            return parser(s)
        except ValueError:
            pass
    return s


def _parse_group(lines):
    tree = {}

    for line in lines:
        match = MTL_PAIRS_RE.findall(line)
        if match:
            key, value = match[0]
            if key == 'GROUP':
                tree[value] = _parse_group(lines)
            elif key == 'END_GROUP':
                break
            else:
                tree[key] = _parse_value(value)
    return tree


def get_geo_ref_points(info):
    return {
        'ul': {'x': info['CORNER_UL_PROJECTION_X_PRODUCT'], 'y': info['CORNER_UL_PROJECTION_Y_PRODUCT']},
        'ur': {'x': info['CORNER_UR_PROJECTION_X_PRODUCT'], 'y': info['CORNER_UR_PROJECTION_Y_PRODUCT']},
        'll': {'x': info['CORNER_LL_PROJECTION_X_PRODUCT'], 'y': info['CORNER_LL_PROJECTION_Y_PRODUCT']},
        'lr': {'x': info['CORNER_LR_PROJECTION_X_PRODUCT'], 'y': info['CORNER_LR_PROJECTION_Y_PRODUCT']},
    }


def get_coords(geo_ref_points, spatial_ref):
    t = osr.CoordinateTransformation(spatial_ref, spatial_ref.CloneGeogCS())

    def transform(p):
        lon, lat, z = t.TransformPoint(p['x'], p['y'])
        return {'lon': lon, 'lat': lat}

    return {key: transform(p) for key, p in geo_ref_points.items()}


def satellite_ref(sat, instrument, file_name):
    """
    To load the band_names for referencing either LANDSAT8 or LANDSAT7 or LANDSAT5 bands
    Landsat7 and Landsat5 have same band names
    """

    name = file_name.stem
    name_len = name.split('_')
    if sat == 'LANDSAT_8':
        if instrument == 'TIRS':
            sat_img = TIRS_ONLY
        else:
            sat_img = images1
    elif len(name_len) > 7:
        sat_img = LANDSAT_BANDS
    else:
        sat_img = LANDSAT_BANDS[:6]
    return sat_img


def get_mtl_content(path):
    """
    Path is pointing to the folder , where the USGS Landsat scene list in MTL format is downloaded
    from Earth Explorer or GloVis
    """

    with path.open('r') as fp:
        mtl_tree = _parse_group(fp)['L1_METADATA_FILE']

    return mtl_tree, path


def prepare_dataset(path):

    info, fileinfo = get_mtl_content(path)

    if info != "Empty File":
        info_pm = info['PRODUCT_METADATA']
        level = info_pm['DATA_TYPE']

        data_format = info_pm['OUTPUT_FORMAT']
        if data_format.upper() == 'GEOTIFF':
            data_format = 'GeoTIFF'

        sensing_time = info_pm['DATE_ACQUIRED'] + ' ' + info_pm['SCENE_CENTER_TIME']

        cs_code = 32600 + info['PROJECTION_PARAMETERS']['UTM_ZONE']
        spatial_ref = osr.SpatialReference()
        spatial_ref.ImportFromEPSG(cs_code)

        geo_ref_points = get_geo_ref_points(info_pm)
        satellite = info_pm['SPACECRAFT_ID']
        instrument = info_pm['SENSOR_ID']

        images = satellite_ref(satellite, instrument, fileinfo)
        return {
            'id': str(uuid.uuid5(uuid.NAMESPACE_URL, path.as_posix())),
            'processing_level': level,
            'product_type': 'LS_USGS_L1C1',
            'label': info['METADATA_FILE_INFO']['LANDSAT_SCENE_ID'],
            'platform': {'code': satellite},
            'instrument': {'name': instrument},
            'extent': {
                'from_dt': sensing_time,
                'to_dt': sensing_time,
                'center_dt': sensing_time,
                'coord': get_coords(geo_ref_points, spatial_ref),
            },
            'format': {'name': data_format},
            'grid_spatial': {
                'projection': {
                    'geo_ref_points': geo_ref_points,
                    'spatial_reference': 'EPSG:%s' % cs_code,
                }
            },
            'image': {
                'bands': {
                    image[1]: {
                        'path': info_pm['FILE_NAME_BAND_' + image[0]],
                        'layer': 1,
                    } for image in images
                }
            },
            'other_metadata': info,
            'lineage': {'source_datasets': {}},
        }


def absolutify_paths(doc, path):

    for band in doc['image']['bands'].values():
        band['path'] = os.path.join(path, band['path'])
    return doc


@click.command(help="""\b
                    Prepare USGS Landsat Collection 1 data for ingestion into the Data Cube.
                    This prepare script supports only for MTL.txt metadata file
                    To Set the Path for referring the datasets -
                    Download the  Landsat scene data from Earth Explorer or GloVis into
                    'some_space_available_folder' and unpack the file.
                    For example: yourscript.py --output [Yaml- which writes datasets into this file for indexing]
                    [Path for dataset as : /home/some_space_available_folder/]""")
@click.option('--output', help="Write output into this directory",
               type=click.Path(exists=False, writable=True, dir_okay=True))
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@click.option('--date', type=Datetime(format='%d/%m/%Y'),
              default=datetime.now(),
              help="Enter file creation start date for data preparation")
@click.option('--checksum/--no-checksum',
              help="Checksum the input dataset to confirm match",
              default=False)
def main(output, datasets, checksum, date):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        level=logging.INFO)

    for ds in datasets:
        (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat(ds)
        create_date = datetime.utcfromtimestamp(ctime)
        if create_date <= date:
            logging.info("Dataset creation time ", create_date,
                         " is older than start date ", date, "...SKIPPING")
        else:
            ds_path = Path(ds)
            if ds_path.suffix in ('MTL.txt'):
                mtl_path = str(ds_path)

                logging.info("Processing %s", ds_path.parent.as_posix())
                output_yaml = pjoin(output, '{}.yaml'.format(ds_path.parent.name))
                logging.info("Output %s", output_yaml)
                if os.path.exists(output_yaml):
                    logging.info("Output already exists %s", output_yaml)
                    with open(output_yaml) as f:
                        if checksum:
                            logging.info("Running checksum comparison")
                            datamap = yaml.load_all(f)
                            for data in datamap:
                                yaml_sha1 = data['checksum_sha1']
                                checksum_sha1 = hashlib.sha1(open(ds, 'rb').read()).hexdigest()
                            if checksum_sha1 == yaml_sha1:
                                logging.info("Dataset preparation already done...SKIPPING")
                                continue
                        else:
                            logging.info("Dataset preparation already done...SKIPPING")
                            continue
                docs = absolutify_paths(prepare_dataset(mtl_path), ds_path.parent)
                with open(output_yaml, 'w') as stream:
                    yaml.dump(docs, stream)


if __name__ == "__main__":
    main()
