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
    # TODO have properly defined product types for the ARD product
    ptype = {'LANDSAT_5': 'L5ARD',
             'LANDSAT_7': 'L7ARD',
             'LANDSAT_8': 'L8ARD',
             'SENTINEL_2A': 'S2MSIARD',
             'SENTINEL_2B': 'S2MSIARD'}

    source_tags = {level1_tags['product_type']: copy.deepcopy(level1_tags)}

    # TODO: extend yaml document to include fmask and gqa yamls
    # Merge tags from each input and create a UUID
    merged_yaml = {
        'algorithm_information': wagl_tags['algorithm_information'],
        'software_versions': wagl_tags['software_versions'],
        'source_data': wagl_tags['source_data'],
        'system_information': wagl_tags['system_information'],
        'id': str(uuid.uuid4()),
        'processing_level': 'Level-2',
        'product_type': ptype[wagl_tags['source_data']['platform_id']],
        'platform': wagl_tags['source_data']['platform_id'],
        'instrument': wagl_tags['source_data']['sensor_id'],
        'format': {'name': 'GeoTIFF'},
        'tile_id': granule,
        'extent': level1_tags['extent'],
        'grid_spatial': level1_tags['grid_spatial'],
        'image': {
            'bands': image_paths},
        'lineage': {
            'ancillary': wagl_tags['ancillary'],
            'source_datasets': source_tags},
        }

    return merged_yaml
