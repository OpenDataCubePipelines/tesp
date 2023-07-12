import tempfile
from pathlib import Path
from typing import Dict

from eodatasets3 import serialise
from eodatasets3.prepare.landsat_l1_prepare import prepare_and_write as ls_prepare
from eodatasets3.prepare.sentinel_l1_prepare import prepare_and_write as s2_prepare
from wagl.acquisition import Acquisition


def extract_level1_metadata(acq: Acquisition) -> Dict:
    """
    Factory method for selecting a level1 metadata script

    Returns the serialisable yaml document(s). Dict, or list of dicts.
    """
    # Optional (not installed yet on Travis)
    # pytest: disable=import-error
    from wagl.acquisition.sentinel import (
        _Sentinel2SinergiseAcquisition,
        Sentinel2Acquisition,
    )
    from wagl.acquisition.landsat import LandsatAcquisition

    with tempfile.TemporaryDirectory() as tmpdir:
        yaml_path = Path(tmpdir) / "level1.yaml"

        if isinstance(acq, _Sentinel2SinergiseAcquisition):
            dataset_doc, path = s2_prepare(
                acq.pathname, yaml_path, producer="sinergise.com", embed_location=True
            )
        elif isinstance(acq, Sentinel2Acquisition):
            dataset_doc, path = s2_prepare(
                acq.pathname, yaml_path, producer="esa.int", embed_location=True
            )
        elif isinstance(acq, LandsatAcquisition):
            uuid, path = ls_prepare(
                acq.pathname, yaml_path, producer="usgs.gov", embed_location=True
            )
            dataset_doc = serialise.from_path(path, skip_validation=True)
        else:
            raise NotImplementedError(
                "No level-1 YAML generation defined for target acquisition "
                "and no yaml_dir defined for level-1 metadata"
            )

        return serialise.to_doc(dataset_doc)
