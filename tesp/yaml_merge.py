# coding=utf-8
"""
Preparation code supporting merge of target Analysis Ready Data yaml metadata document
and the source Level 1 yaml

example usage:
    python yaml_merge.py /g/data/v10/tmp/S2A_OPER_MSI_ARD_TL_SGS__20160703T061054_A005376_T52KGA_N02.04/ARD-METADATA.yaml
        --source /g/data/v10/AGDCv2/indexed_datasets/cophub/s2/s2_l1c_yamls/
"""  # nopep8
from __future__ import absolute_import

import logging
import os
import uuid
import copy
import re

import click
import yaml

os.environ["CPL_ZIP_ENCODING"] = "UTF-8"


def provider_reference_info(granule, wagl_tags):
    """
    Extracts provider reference metadata
    Supported platforms are:
        * LANDSAT
        * SENTINEL2
    :param granule:
        A string referring to the name of the capture
    
    :return: 
        Dictionary; contains satellite reference if identified
    """
    provider_info = {}
    matches = None
    if 'LANDSAT' in wagl_tags['source_datasets']['platform_id']:
        matches = re.match(r'L\w\d(?P<reference_code>\d{6}).*', granule)
    elif 'SENTINEL2' in wagl_tags['source_datasets']['platform_id']:
        matches = re.match(r'.*_T(?P<reference_code>\d{1,2}[A-Z]{3})_.*')
    
    if matches:
        provider_info.update(**matches.groupdict())
    return provider_info


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
    provider_info = provider_refernce_info(granule, wagl_tags)

    # TODO: extend yaml document to include fmask and gqa yamls
    # Merge tags from each input and create a UUID
    merged_yaml = {
        'algorithm_information': wagl_tags['algorithm_information'],
        'software_versions': wagl_tags['software_versions'],
        'system_information': wagl_tags['system_information'],
        'id': str(uuid.uuid4()),
        'processing_level': 'Level-2',
        'product_type': ptype[wagl_tags['source_datasets']['platform_id']],
        'platform': {'code': wagl_tags['source_datasets']['platform_id']},
        'instrument': {'name': wagl_tags['source_datasets']['sensor_id']},
        'format': {'name': 'GeoTIFF'},
        'tile_id': granule,
        'extent': level1_tags['extent'],
        'grid_spatial': level1_tags['grid_spatial'],
        'image': {'bands': image_paths},
        'lineage': {
            'ancillary': wagl_tags['ancillary'],
            'source_datasets': source_tags
        },
    }

    if provider_info:
        merged_yaml['provider'] = provider_info

    return merged_yaml
