from pathlib import Path

from .ls_usgs_l1_prepare import prepare_dataset as landsat_prepare
from .s2_prepare_cophub_zip import prepare_dataset as sentinel_2_zip_prepare
from .s2_l1c_aws_pds_prepare import prepare_dataset as sentinel_2_aws_pds_prepare

from wagl.acquisition.sentinel import _Sentinel2SinergiseAcquisition, Sentinel2Acquisition
from wagl.acquisition.landsat import LandsatAcquisition
from wagl.acquisition import find_in


def extract_level1_metadata(acq, acquisition_path):
    """
    Factory method for selecting a level1 metadata script

    """
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
