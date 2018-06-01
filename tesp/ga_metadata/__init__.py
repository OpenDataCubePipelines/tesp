from .ls_usgs_l1_prepare import prepare as landsat_prepare
from .s2_prepare_cophub_zip import prepare_dataset as sentinel_2_zip_prepare
from .s2_l1c_aws_pds_prepare import prepare_dataset as sentinel_2_aws_pds_prepare

from ..acquisitions.sentinel import _Sentinel2SinergiseAcquisition, Sentinel2Acquisition
from ..acquisitions.landsat import LandsatAcquisition
from ..acquisitions import find_in


def extract_level1_metadata(acq, acquisition_path):
    """
    Factory method for selecting a level1 metadata script

    """
    if isinstance(acq, _Sentinel2SinergiseAcquisition):
        return sentinel_2_aws_pds_prepare(acq.granule_xml)
    elif isinstance(acq, Sentinel2Acquisition):
        return sentinel_2_zip_prepare(acq.granule_xml)
    elif isinstance(acq, LandsatAcquisition):
        mtl_file = find_in(acquisition_path, 'MTL')
        return landsat_prepare(mtl_file)

    raise NotImplementedError('No level1 yaml generation defined for target acquisition')
