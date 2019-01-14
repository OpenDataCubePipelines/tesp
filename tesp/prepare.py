from pathlib import Path

from eodatasets.prepare.ls_usgs_l1_prepare import prepare_dataset as landsat_prepare
from eodatasets.prepare.s2_prepare_cophub_zip import prepare_dataset as sentinel_2_zip_prepare
from eodatasets.prepare.s2_l1c_aws_pds_prepare import prepare_dataset as sentinel_2_aws_pds_prepare


def extract_level1_metadata(acq, acquisition_path):
    """
    Factory method for selecting a level1 metadata script

    """
    # Optional (not installed yet on Travis)
    # pytest: disable=import-error
    from wagl.acquisition.sentinel import (
        _Sentinel2SinergiseAcquisition, Sentinel2Acquisition
    )
    from wagl.acquisition.landsat import LandsatAcquisition


    if isinstance(acq, _Sentinel2SinergiseAcquisition):
        return sentinel_2_aws_pds_prepare(Path(acq.granule_xml))
    elif isinstance(acq, Sentinel2Acquisition):
        return sentinel_2_zip_prepare(Path(acq.pathname))
    elif isinstance(acq, LandsatAcquisition):
        return landsat_prepare(Path(acquisition_path))

    raise NotImplementedError(
        'No level-1 YAML generation defined for target acquisition '
        'and no yaml_dir defined for level-1 metadata'
    )
