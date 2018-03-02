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


def merge_metadata(level1_tags, wagl_tags, granule, image_paths):
    """
    Combine the metadata from input sources and output
    into a single ARD metadata yaml.
    """
    # TODO enforce metdata is generated upstream and passed to here
    if level1_tags is None:
        extent = {}
        grid_spatial = {}
        tile_reference = {}
        source_datasets = {}
        cloud_pct = {}
    else:
        extent = level1_tags['extent']
        grid_spatial = level1_tags['grid_spatial']
        tile_reference = level1_tags['image']['tile_reference']
        source_datasets = {'S2MSI1C': copy.deepcopy(level1_tags)}
        cloud_pct = level1_tags['image']['cloud_cover_percentage']

    # TODO product_type, source_datasets key name

    # TODO: extend yaml document to include fmask and gqa yamls
    # Merge tags from each input and create a UUID
    merged_yaml = {
        'algorithm_information': wagl_tags['algorithm_information'],
        'software_versions': wagl_tags['software_versions'],
        'source_data': wagl_tags['source_data'],
        'system_information': wagl_tags['system_information'],
        'id': str(uuid.uuid4()),
        'processing_level': 'Level-2',
        'product_type': 'S2MSIARD',
        'platform': wagl_tags['source_data']['platform_id'],
        'instrument': wagl_tags['source_data']['sensor_id'],
        'format': {'name': 'GeoTIFF'},
        'tile_id': granule,
        'extent': extent,
        'grid_spatial': grid_spatial,
        'image': {
            'tile_reference': tile_reference,
            'cloud_cover_percentage': cloud_pct,
            'bands': image_paths},
        'lineage': {
            'source_datasets': source_datasets},
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
