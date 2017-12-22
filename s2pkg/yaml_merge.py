# coding=utf-8
"""
Preparation code supporting merge of target Analysis Ready Data yaml metadata document
and the source Level 1 yaml

example usage:
    python yaml_merge.py /g/data/v10/tmp/S2A_OPER_MSI_ARD_TL_SGS__20160703T061054_A005376_T52KGA_N02.04/ARD-METADATA.yaml
        --source /g/data/v10/AGDCv2/indexed_datasets/cophub/s2/s2_l1c_yamls/
"""
from __future__ import absolute_import
import logging
import os
import uuid
import copy
import click
import yaml
os.environ["CPL_ZIP_ENCODING"] = "UTF-8"


def image_dict(target, root_path=None):
    """
    Returns a datacube-compatible dictionary of TIF image paths
    """
    # method of using relative paths
    if root_path is None:
        root_path = ''

    nbar_match_dict = {'blue': 'B02',
                       'green': 'B03',
                       'red': 'B04',
                       'nir': 'B08',
                       'rededge1': 'B05',
                       'rededge2': 'B06',
                       'rededge3': 'B07',
                       'rededge4': 'B8A',
                       'swir1': 'B11',
                       'swir2': 'B12',
                       'aerosol': 'B01',
                       'contiguity': 'CONTIGUITY'}

    nbart_match_dict = {'t_blue': 'B02',
                        't_green': 'B03',
                        't_red': 'B04',
                        't_nir': 'B08',
                        't_rededge1': 'B05',
                        't_rededge2': 'B06',
                        't_rededge3': 'B07',
                        't_rededge4': 'B8A',
                        't_swir1': 'B11',
                        't_swir2': 'B12',
                        't_aerosol': 'B01',
                        't_contiguity': 'CONTIGUITY'}

    pq_match_dict = {'pixel_quality': 'QA'}

    img_dict = {}

    # pixel quality datasets
    for file in os.listdir(target):
        if '.TIF' in file:
            for band_label, band_name in pq_match_dict.items():
                if band_name in file:
                    fname = os.path.join(root_path, file)
                    img_dict[band_label] = {'path': fname, 'layer': 1}

    # nbar datasets
    nbar_target = os.path.join(target, 'NBAR')
    for file in os.listdir(nbar_target):
        if '.TIF' in file:
            for band_label, band_name in nbar_match_dict.items():
                if band_name in file:
                    fname = os.path.join(root_path, 'NBAR', file)
                    img_dict[band_label] = {'path': fname, 'layer': 1}

    # nbart datasets
    nbart_target = os.path.join(target, 'NBART')
    for file in os.listdir(nbart_target):
        if '.TIF' in file:
            for band_label, band_name in nbart_match_dict.items():
                if band_name in file:
                    fname = os.path.join(root_path, 'NBART', file)
                    img_dict[band_label] = {'path': fname, 'layer': 1}

    return img_dict


def merge_metadata(level1_tags, gaip_tags, package_dir, root_path=None):
    """
    Combine the metadata from input sources and output
    into a single ARD metadata yaml.
    """
    # method of using relative paths
    if root_path is None:
        root_path = ''

    # TODO: extend yaml document to include fmask and gqa yamls
    # Merge tags from each input and create a UUID
    merged_yaml = {
        'algorithm_information': gaip_tags['algorithm_information'],
        'software_versions': gaip_tags['software_versions'],
        'source_data': gaip_tags['source_data'],
        'system_information': gaip_tags['system_information'],
        'id': str(uuid.uuid4()),
        'processing_level': 'Level-2',
        'product_type': 'S2MSIARD',
        'platform': level1_tags['platform'],
        'instrument': level1_tags['instrument'],
        'format': {'name': 'GeoTIFF'},
        'tile_id': level1_tags['tile_id'],
        'extent': level1_tags['extent'],
        'grid_spatial': level1_tags['grid_spatial'],
        'image': {
            'tile_reference': level1_tags['image']['tile_reference'],
            'cloud_cover_percentage': level1_tags['image']['cloud_cover_percentage'],
            'bands': image_dict(package_dir, root_path)},
        'lineage': {
            'source_datasets': {'S2MSI1C': copy.deepcopy(level1_tags)}},
        }

    return merged_yaml


def merge(target_yaml, source_root):
    """
    Returns a dictionary of datacube-compatible merged target and source metadata
    """
    with open(target_yaml, 'r') as stream:
        target = yaml.load(stream.read())
        root_dir = os.path.dirname(target['source_data']['source_level1'])
        root_dir = os.path.basename(root_dir)
        basename = os.path.basename(target['source_data']['source_level1'])

    source_yaml = os.path.join(source_root,
                               os.path.join(source_root,
                                            root_dir,
                                            basename + ".yaml"))
    target_root = os.path.dirname(target_yaml)
    target_basename = os.path.basename(target_root)
    granule = target_basename.replace('ARD', 'L1C')
    for document in yaml.load_all(open(source_yaml)):
        if granule == document['tile_id']:
            source = document

    merged_yaml = merge_metadata(source, target, target_root)

    return merged_yaml


@click.command(help=__doc__)
@click.argument('targets',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@click.option('--source', help="Root path to level 1 prepare yamls",
              type=click.Path(exists=False, writable=True, dir_okay=True))
def main(targets, source):
    """
    For each yaml in input 'targets' update it's content to include the level 1 soruce content available at
    'source' and overwrite the input yaml
    """
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
    for target_yaml in targets:
        target_yaml = os.path.abspath(target_yaml)
        merged_yaml = merge(target_yaml, source)
        with open(target_yaml, 'w') as outfile:
            yaml.safe_dump(merged_yaml, outfile, default_flow_style=False)
        logging.info("YAML target merge with source successful. Bye!")

if __name__ == "__main__":
    main()
