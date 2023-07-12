# coding=utf-8
"""
Preparation code supporting merge of target Analysis Ready Data yaml metadata document
and the source Level 1 yaml
"""  # nopep8
from __future__ import absolute_import

import os
import uuid
import copy
import re
import numpy as np
from tesp.metadata import _get_tesp_metadata as tesp_version

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
    if "LANDSAT" in wagl_tags["source_datasets"]["platform_id"]:
        matches = re.match(r"L\w\d(?P<reference_code>\d{6}).*", granule)
    elif "SENTINEL_2" in wagl_tags["source_datasets"]["platform_id"]:
        matches = re.match(r".*_T(?P<reference_code>\d{1,2}[A-Z]{3})_.*", granule)

    if matches:
        provider_info.update(**matches.groupdict())
    return provider_info


def merge_metadata(
    level1_tags, wagl_tags, granule, image_paths, platform, **antecedent_tags
):
    """
    Combine the metadata from input sources and output
    into a single ARD metadata yaml.
    """
    # TODO have properly defined product types for the ARD product
    ptype = {
        "LANDSAT_5": "L5ARD",
        "LANDSAT_7": "L7ARD",
        "LANDSAT_8": "L8ARD",
        "SENTINEL_2A": "S2MSIARD",
        "SENTINEL_2B": "S2MSIARD",
    }

    # TODO: resolve common software version for fmask and gqa
    software_versions = wagl_tags["software_versions"]

    tesp_metadata = tesp_version()
    software_versions["tesp"] = tesp_metadata["software_versions"]["tesp"]

    # for Landsat, from_dt and to_dt in ARD-METADATA is populated from max and min timedelta values
    if platform == "LANDSAT":
        # pylint: disable=too-many-function-args
        def interpret_landsat_temporal_extent():
            """
            Landsat imagery only provides a center datetime; a time range can be derived
            from the timedelta dataset
            """

            center_dt = np.datetime64(level1_tags["extent"]["center_dt"])
            from_dt = center_dt + np.timedelta64(
                int(float(wagl_tags.pop("timedelta_min")) * 1000000), "us"
            )
            to_dt = center_dt + np.timedelta64(
                int(float(wagl_tags.pop("timedelta_max")) * 1000000), "us"
            )

            level2_extent = {
                "center_dt": "{}Z".format(center_dt),
                "coord": level1_tags["extent"]["coord"],
                "from_dt": "{}Z".format(from_dt),
                "to_dt": "{}Z".format(to_dt),
            }

            return level2_extent

        level2_extent = interpret_landsat_temporal_extent()
    else:
        level2_extent = level1_tags["extent"]

    # TODO: extend yaml document to include fmask and gqa yamls
    merged_yaml = {
        "algorithm_information": wagl_tags["algorithm_information"],
        "system_information": wagl_tags["system_information"],
        "id": str(uuid.uuid4()),
        "processing_level": "Level-2",
        "product_type": ptype[wagl_tags["source_datasets"]["platform_id"]],
        "platform": {"code": wagl_tags["source_datasets"]["platform_id"]},
        "instrument": {"name": wagl_tags["source_datasets"]["sensor_id"]},
        "parameters": wagl_tags["parameters"],
        "format": {"name": "GeoTIFF"},
        "tile_id": granule,
        "extent": level2_extent,
        "grid_spatial": level1_tags["grid_spatial"],
        "image": {"bands": image_paths},
        "lineage": {
            "ancillary": wagl_tags["ancillary"],
            "source_datasets": {level1_tags["product_type"]: copy.deepcopy(level1_tags)},
        },
    }

    # Configured to handle gqa and fmask antecedent tasks
    for task_name, task_md in antecedent_tags.items():
        if "software_versions" in task_md:
            for key, value in task_md.pop("software_versions").items():
                software_versions[key] = value  # This fails on key conflicts

        # Check for valid metadata after merging the software versions
        if task_md:
            merged_yaml[task_name] = task_md

    provider_info = provider_reference_info(granule, wagl_tags)
    if provider_info:
        merged_yaml["provider"] = provider_info

    merged_yaml["software_versions"] = software_versions

    return merged_yaml
