"""
Ingest data from the command-line.
"""
from __future__ import absolute_import

import uuid
import logging
import yaml
import re
import click
from osgeo import osr
import os
from pathlib import Path
from click_datetime import Datetime
from datetime import datetime
from os.path import join as pjoin
import hashlib
import tarfile
import glob

images1 = [('1', 'coastal_aerosol'),
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

images2 = [('1', 'blue'),
           ('2', 'green'),
           ('3', 'red'),
           ('4', 'nir'),
           ('5', 'swir1'),
           ('7', 'swir2'),
           ('QUALITY', 'quality')]

try:
    from urllib.request import urlopen
    from urllib.parse import urlparse, urljoin
except ImportError:
    from urlparse import urlparse, urljoin
    from urllib2 import urlopen

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

#        match = MTL_PAIRS_RE.findall(line.decode('utf-8'))
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


def satellite_ref(sat, file_name):
    """
    To load the band_names for referencing either LANDSAT8 or LANDSAT7 or LANDSAT5 bands
    Landsat7 and Landsat5 have same band names
    """
    
    name = (Path(file_name)).stem
    name_len = name.split('_')
    if sat == 'LANDSAT_8':
        sat_img = images1
    elif len(name_len) > 7:
        sat_img = images2
    else:
        sat_img = images2[:6]
    return sat_img


def get_mtl(path):
    """
    Path is pointing to the folder , where the USGS Landsat scene list in MTL format is downloaded
    from Earth Explorer or GloVis
    """
    
    newfile = "Empty File"
    metafile = "Name_of_File"
    if os.path.isdir(path):
        for file in os.listdir(path):
            if file.endswith("MTL.txt"):
                metafile = file
                newfile = open(os.path.join(path, metafile), 'rb')
                newfile = _parse_group(newfile)['L1_METADATA_FILE']
    return newfile, metafile


def get_mtl_content(path):
    """
    Path is pointing to the folder , where the USGS Landsat scene list in MTL format is downloaded
    from Earth Explorer or GloVis
    """
    
    newfile = "Empty File"
    metafile = "Name_of_File"
    
    metafile = path
    newfile = open(metafile, 'r')
    newfile = _parse_group(newfile)['L1_METADATA_FILE']
    
    return newfile, metafile
    
    
def prepare_dataset(path):
    
    #info, fileinfo = get_mtl(path)
    info, fileinfo = get_mtl_content(path)
    
    if info != "Empty File":
    # Copying [PRODUCT_METADATA] group into 'info_pm'
        info_pm = info['PRODUCT_METADATA']
        level = info_pm['DATA_TYPE']
        #product_type = info_pm['DATA_TYPE']
        
        data_format = info_pm['OUTPUT_FORMAT']
        if data_format.upper() == 'GEOTIFF':
            data_format = 'GeoTiff'
    
        sensing_time = info_pm['DATE_ACQUIRED'] + ' ' + info_pm['SCENE_CENTER_TIME']
    
        cs_code = 32600 + info['PROJECTION_PARAMETERS']['UTM_ZONE']
        spatial_ref = osr.SpatialReference()
        spatial_ref.ImportFromEPSG(cs_code)
    
        geo_ref_points = get_geo_ref_points(info_pm)
        satellite = info_pm['SPACECRAFT_ID']
    
        images = satellite_ref(satellite, fileinfo)
        return {
            'id': str(uuid.uuid5(uuid.NAMESPACE_URL, path)),
            'processing_level': level,
            'product_type': 'LS_USGS_L1C1',
            # 'creation_dt': ct_time,
            'label': info['METADATA_FILE_INFO']['LANDSAT_SCENE_ID'],
            'platform': {'code': satellite},
            'instrument': {'name': info_pm['SENSOR_ID']},
            # 'acquisition': {'groundstation': {'code': station}},
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
                    #     'valid_data': {
                    #         'coordinates': tileInfo['tileDataGeometry']['coordinates'],
                    #         'type': tileInfo['tileDataGeometry']['type']}
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


def absolutify_paths(doc, ds_path):
    if Path(ds_path).suffix != '.gz':
        for band in doc['image']['bands'].values():             
            band['path'] = os.path.join(str(Path(ds_path)), band['path'])
    else:
        for band in doc['image']['bands'].values():             
            band['path'] = 'tar:{}!{}'.format(ds_path, band['path'])
    return doc


def find_gz_mtl(ds_path, output_folder):
    """
    Find the MTL metadata file for the archived dataset and extract the xml 
    file and store it temporally in output folder

    :param ds_path: the dataset path
    :param output_folder: the output folder

    :returns: xml with full path 

    """
 
    mtl_path = ''
    
    reT = re.compile("MTL.txt")
    tar_gz = tarfile.open(str(ds_path), 'r')
    members=[m for m in tar_gz.getmembers() if reT.search(m.name)]
    tar_gz.extractall(output_folder, members)
    mtl_path = pjoin(output_folder, members[0].name)
        
    return mtl_path
    

@click.command(help="""\b
                    Prepare USGS Landsat Collection 1 data for ingestion into the Data Cube.
                    This prepare script supports only for MTL.txt metadata file
                    To Set the Path for referring the datasets -
                    Download the  Landsat scene data from Earth Explorer or GloVis into
                    'some_space_available_folder' and unpack the file.
                    For example: yourscript.py --output [Yaml- which writes datasets into this file for indexing]
                    [Path for dataset as : /home/some_space_available_folder/]""")


#@click.command(help=__doc__)  
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
            #if ds_path.suffix in ('.gz', 'MTL.txt'):
            if ds_path.suffix in ('.gz', '.txt'):                 
                if ds_path.suffix != '.txt': 
                    mtl_path = find_gz_mtl(ds_path, output)
                    if mtl_path == '':
                        raise RuntimeError('no MTL file under the product folder')
                else:
                    mtl_path = str(ds_path)                       
                    ds_path = os.path.dirname(str(ds_path))                    
             
                #print (mtl_path)                 
                logging.info("Processing %s", ds_path) 
                output_yaml = pjoin(output, '{}.yaml'.format(os.path.basename(mtl_path).replace('_MTL.txt', '')))
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
                docs = absolutify_paths(prepare_dataset(mtl_path), ds_path)
                with open(output_yaml, 'w') as stream:
                    yaml.dump(docs, stream)

    #delete intermediate MTL files for archive datasets in output folder
    mtl_list = glob.glob('{}/*MTL.txt'.format(output))
    if len(mtl_list) > 0:
        for f in mtl_list:
            try:
                os.remove(f)
            except OSError:
                pass


if __name__ == "__main__":
    main()
