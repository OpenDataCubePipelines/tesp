import pytest
from pathlib import Path
from eodatasets3.wagl import Granule
from datetime import datetime, timezone
from tesp.package import package_non_standard
import yaml

h5py = pytest.importorskip(
    "h5py",
    reason="Extra dependencies needed to run wagl package test."
    "Try pip install eodatasets3[wagl]",
)

# This test dataset comes from running 'tests/integration/h5downsample.py' on
# a real wagl output.
WAGL_INPUT_PATH: Path = Path(
    __file__
).parent / "data/wagl-input/LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"

# The matching Level1 metadata (produced by landsat_l1_prepare.py)
L1_METADATA_PATH: Path = Path(
    __file__
).parent / "data/wagl-input/LC08_L1TP_092084_20160628_20170323_01_T1.yaml"

# The fmask image
FMASK_IMAGE_PATH: Path = Path(
    __file__
).parent / "data/wagl-input/LC80920842016180LGN01/LC80920842016180LGN01.fmask.img"

# The fmask document path
FMASK_DOC_PATH: Path = Path(
    __file__
).parent / "data/wagl-input/LC80920842016180LGN01/LC80920842016180LGN01.fmask.yaml"

# The gqa document path
GQA_DOC_PATH: Path = Path(
    __file__
).parent / "data/wagl-input/LC80920842016180LGN01/LC80920842016180LGN01.gqa.yaml"

# tesp document path
TESP_DOC_PATH: Path = Path(
    __file__
).parent / "data/wagl-input/LC80920842016180LGN01/LC80920842016180LGN01.tesp.yaml"

granule_names = ["LC80920842016180LGN01"]


@pytest.fixture()
def expected_wagl_output(tmp_path):
    return {
        "$schema": "https://schemas.opendatacube.org/dataset",
        "accessories": {},
        "crs": "epsg:32655",
        "geometry": {
            "coordinates": [
                [
                    [593115.0, -3713085.0],
                    [360585.0, -3713085.0],
                    [360585.0, -3947415.0],
                    [593115.0, -3947415.0],
                    [593115.0, -3713085.0],
                ]
            ],
            "type": "Polygon",
        },
        "grids": {
            "default": {
                "shape": [79, 78],
                "transform": [
                    2981.153846153846,
                    0.0,
                    360585.0,
                    0.0,
                    -2966.2025316455697,
                    -3713085.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "rg0": {
                "shape": [157, 156],
                "transform": [
                    1490.4807692307693,
                    0.0,
                    360592.5,
                    0.0,
                    -1492.452229299363,
                    -3713092.5,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "rg0_oa_dsm_smoothed": {
                "shape": [167, 166],
                "transform": [
                    1497.198795180723,
                    0.0,
                    352582.5,
                    0.0,
                    -1499.0119760479042,
                    -3705082.5,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            "rg1_oa_dsm_smoothed": {
                "shape": [84, 83],
                "transform": [
                    2994.5783132530123,
                    0.0,
                    352575.0,
                    0.0,
                    -2980.357142857143,
                    -3705075.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
        },
        "label": "ga_ls8c_ard_3-1-0_092084_2016-06-28",
        "lineage": {"level1": ["fb1c622e-90aa-50e8-9d5e-ad69db82d0f6"]},
        "measurements": {
            "oa_fmask": {
                "layer": "//LC80920842016180LGN01/OA_FMASK/oa_fmask",
                "path": str(
                    tmp_path
                    / "LC80920842016180LGN01/LC80920842016180LGN01.converted.datasets.h5"  # noqa: W503
                ),
            },
            "rg0_lambertian_panchromatic": {
                "grid": "rg0",
                "layer": "//LC80920842016180LGN01/RES-GROUP-0/STANDARDISED-PRODUCTS/REFLECTANCE/LAMBERTIAN/BAND-8",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg0_nbar_panchromatic": {
                "grid": "rg0",
                "layer": "//LC80920842016180LGN01/RES-GROUP-0/STANDARDISED-PRODUCTS/REFLECTANCE/NBAR/BAND-8",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg0_nbart_panchromatic": {
                "grid": "rg0",
                "layer": "//LC80920842016180LGN01/RES-GROUP-0/STANDARDISED-PRODUCTS/REFLECTANCE/NBART/BAND-8",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg0_oa_a_panchromatic": {
                "grid": "rg0",
                "layer": "//LC80920842016180LGN01/RES-GROUP-0/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/A/BAND-8",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg0_oa_aspect": {
                "grid": "rg0",
                "layer": "//LC80920842016180LGN01/RES-GROUP-0/SLOPE-ASPECT/ASPECT",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg0_oa_azimuthal_exiting": {
                "grid": "rg0",
                "layer": "//LC80920842016180LGN01/RES-GROUP-0/EXITING-ANGLES/AZIMUTHAL-EXITING",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg0_oa_azimuthal_incident": {
                "grid": "rg0",
                "layer": "//LC80920842016180LGN01/RES-GROUP-0/INCIDENT-ANGLES/AZIMUTHAL-INCIDENT",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg0_oa_b_panchromatic": {
                "grid": "rg0",
                "layer": "//LC80920842016180LGN01/RES-GROUP-0/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/B/BAND-8",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg0_oa_cast_shadow_satellite": {
                "grid": "rg0",
                "layer": "//rg0_oa_cast_shadow_satellite",
                "path": str(
                    tmp_path
                    / "LC80920842016180LGN01/LC80920842016180LGN01.converted.datasets.h5"  # noqa: W503
                ),
            },
            "rg0_oa_cast_shadow_sun": {
                "grid": "rg0",
                "layer": "//rg0_oa_cast_shadow_sun",
                "path": str(
                    tmp_path
                    / "LC80920842016180LGN01/LC80920842016180LGN01.converted.datasets.h5"  # noqa: W503
                ),
            },
            "rg0_oa_dif_panchromatic": {
                "grid": "rg0",
                "layer": "//LC80920842016180LGN01/RES-GROUP-0/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/DIF/BAND-8",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg0_oa_dir_panchromatic": {
                "grid": "rg0",
                "layer": "//LC80920842016180LGN01/RES-GROUP-0/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/DIR/BAND-8",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg0_oa_dsm_smoothed": {
                "grid": "rg0_oa_dsm_smoothed",
                "layer": "//LC80920842016180LGN01/RES-GROUP-0/ELEVATION/DSM-SMOOTHED",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg0_oa_exiting": {
                "grid": "rg0",
                "layer": "//LC80920842016180LGN01/RES-GROUP-0/EXITING-ANGLES/EXITING-ANGLE",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg0_oa_fs_panchromatic": {
                "grid": "rg0",
                "layer": "//LC80920842016180LGN01/RES-GROUP-0/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/FS/BAND-8",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg0_oa_fv_panchromatic": {
                "grid": "rg0",
                "layer": "//LC80920842016180LGN01/RES-GROUP-0/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/FV/BAND-8",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg0_oa_incident": {
                "grid": "rg0",
                "layer": "//LC80920842016180LGN01/RES-GROUP-0/INCIDENT-ANGLES/INCIDENT-ANGLE",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg0_oa_latitude": {
                "grid": "rg0",
                "layer": "//LC80920842016180LGN01/RES-GROUP-0/LONGITUDE-LATITUDE/LATITUDE",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg0_oa_longitude": {
                "grid": "rg0",
                "layer": "//LC80920842016180LGN01/RES-GROUP-0/LONGITUDE-LATITUDE/LONGITUDE",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg0_oa_relative_azimuth": {
                "grid": "rg0",
                "layer": "//LC80920842016180LGN01/RES-GROUP-0/SATELLITE-SOLAR/RELATIVE-AZIMUTH",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg0_oa_relative_slope": {
                "grid": "rg0",
                "layer": "//LC80920842016180LGN01/RES-GROUP-0/RELATIVE-SLOPE/RELATIVE-SLOPE",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg0_oa_s_panchromatic": {
                "grid": "rg0",
                "layer": "//LC80920842016180LGN01/RES-GROUP-0/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/S/BAND-8",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg0_oa_satellite_azimuth": {
                "grid": "rg0",
                "layer": "//LC80920842016180LGN01/RES-GROUP-0/SATELLITE-SOLAR/SATELLITE-AZIMUTH",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg0_oa_satellite_view": {
                "grid": "rg0",
                "layer": "//LC80920842016180LGN01/RES-GROUP-0/SATELLITE-SOLAR/SATELLITE-VIEW",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg0_oa_self_shadow": {
                "grid": "rg0",
                "layer": "//rg0_oa_self_shadow",
                "path": str(
                    tmp_path
                    / "LC80920842016180LGN01/LC80920842016180LGN01.converted.datasets.h5"  # noqa: W503
                ),
            },
            "rg0_oa_slope": {
                "grid": "rg0",
                "layer": "//LC80920842016180LGN01/RES-GROUP-0/SLOPE-ASPECT/SLOPE",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg0_oa_solar_azimuth": {
                "grid": "rg0",
                "layer": "//LC80920842016180LGN01/RES-GROUP-0/SATELLITE-SOLAR/SOLAR-AZIMUTH",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg0_oa_solar_zenith": {
                "grid": "rg0",
                "layer": "//LC80920842016180LGN01/RES-GROUP-0/SATELLITE-SOLAR/SOLAR-ZENITH",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg0_oa_terrain_shadow": {
                "grid": "rg0",
                "layer": "//rg0_oa_terrain_shadow",
                "path": str(
                    tmp_path
                    / "LC80920842016180LGN01/LC80920842016180LGN01.converted.datasets.h5"  # noqa: W503
                ),
            },
            "rg0_oa_timedelta": {
                "grid": "rg0",
                "layer": "//LC80920842016180LGN01/RES-GROUP-0/SATELLITE-SOLAR/TIME-DELTA",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg0_oa_ts_panchromatic": {
                "grid": "rg0",
                "layer": "//LC80920842016180LGN01/RES-GROUP-0/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/TS/BAND-8",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_lambertian_blue": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/STANDARDISED-PRODUCTS/REFLECTANCE/LAMBERTIAN/BAND-2",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_lambertian_coastal_aerosol": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/STANDARDISED-PRODUCTS/REFLECTANCE/LAMBERTIAN/BAND-1",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_lambertian_green": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/STANDARDISED-PRODUCTS/REFLECTANCE/LAMBERTIAN/BAND-3",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_lambertian_nir": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/STANDARDISED-PRODUCTS/REFLECTANCE/LAMBERTIAN/BAND-5",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_lambertian_red": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/STANDARDISED-PRODUCTS/REFLECTANCE/LAMBERTIAN/BAND-4",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_lambertian_swir_1": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/STANDARDISED-PRODUCTS/REFLECTANCE/LAMBERTIAN/BAND-6",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_lambertian_swir_2": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/STANDARDISED-PRODUCTS/REFLECTANCE/LAMBERTIAN/BAND-7",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_nbar_blue": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/STANDARDISED-PRODUCTS/REFLECTANCE/NBAR/BAND-2",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_nbar_coastal_aerosol": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/STANDARDISED-PRODUCTS/REFLECTANCE/NBAR/BAND-1",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_nbar_green": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/STANDARDISED-PRODUCTS/REFLECTANCE/NBAR/BAND-3",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_nbar_nir": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/STANDARDISED-PRODUCTS/REFLECTANCE/NBAR/BAND-5",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_nbar_red": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/STANDARDISED-PRODUCTS/REFLECTANCE/NBAR/BAND-4",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_nbar_swir_1": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/STANDARDISED-PRODUCTS/REFLECTANCE/NBAR/BAND-6",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_nbar_swir_2": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/STANDARDISED-PRODUCTS/REFLECTANCE/NBAR/BAND-7",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_nbart_blue": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/STANDARDISED-PRODUCTS/REFLECTANCE/NBART/BAND-2",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_nbart_coastal_aerosol": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/STANDARDISED-PRODUCTS/REFLECTANCE/NBART/BAND-1",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_nbart_green": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/STANDARDISED-PRODUCTS/REFLECTANCE/NBART/BAND-3",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_nbart_nir": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/STANDARDISED-PRODUCTS/REFLECTANCE/NBART/BAND-5",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_nbart_red": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/STANDARDISED-PRODUCTS/REFLECTANCE/NBART/BAND-4",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_nbart_swir_1": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/STANDARDISED-PRODUCTS/REFLECTANCE/NBART/BAND-6",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_nbart_swir_2": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/STANDARDISED-PRODUCTS/REFLECTANCE/NBART/BAND-7",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_a_blue": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/A/BAND-2",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_a_coastal_aerosol": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/A/BAND-1",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_a_green": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/A/BAND-3",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_a_nir": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/A/BAND-5",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_a_red": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/A/BAND-4",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_a_swir_1": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/A/BAND-6",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_a_swir_2": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/A/BAND-7",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_aspect": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/SLOPE-ASPECT/ASPECT",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_azimuthal_exiting": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/EXITING-ANGLES/AZIMUTHAL-EXITING",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_azimuthal_incident": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INCIDENT-ANGLES/AZIMUTHAL-INCIDENT",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_b_blue": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/B/BAND-2",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_b_coastal_aerosol": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/B/BAND-1",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_b_green": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/B/BAND-3",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_b_nir": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/B/BAND-5",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_b_red": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/B/BAND-4",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_b_swir_1": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/B/BAND-6",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_b_swir_2": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/B/BAND-7",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_cast_shadow_satellite": {
                "layer": "//rg1_oa_cast_shadow_satellite",
                "path": str(
                    tmp_path
                    / "LC80920842016180LGN01/LC80920842016180LGN01.converted.datasets.h5"  # noqa: W503
                ),
            },
            "rg1_oa_cast_shadow_sun": {
                "layer": "//rg1_oa_cast_shadow_sun",
                "path": str(
                    tmp_path
                    / "LC80920842016180LGN01/LC80920842016180LGN01.converted.datasets.h5"  # noqa: W503
                ),
            },
            "rg1_oa_dif_blue": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/DIF/BAND-2",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_dif_coastal_aerosol": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/DIF/BAND-1",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_dif_green": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/DIF/BAND-3",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_dif_nir": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/DIF/BAND-5",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_dif_red": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/DIF/BAND-4",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_dif_swir_1": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/DIF/BAND-6",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_dif_swir_2": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/DIF/BAND-7",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_dir_blue": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/DIR/BAND-2",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_dir_coastal_aerosol": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/DIR/BAND-1",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_dir_green": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/DIR/BAND-3",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_dir_nir": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/DIR/BAND-5",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_dir_red": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/DIR/BAND-4",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_dir_swir_1": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/DIR/BAND-6",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_dir_swir_2": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/DIR/BAND-7",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_dsm_smoothed": {
                "grid": "rg1_oa_dsm_smoothed",
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/ELEVATION/DSM-SMOOTHED",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_exiting": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/EXITING-ANGLES/EXITING-ANGLE",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_fs_blue": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/FS/BAND-2",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_fs_coastal_aerosol": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/FS/BAND-1",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_fs_green": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/FS/BAND-3",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_fs_nir": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/FS/BAND-5",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_fs_red": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/FS/BAND-4",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_fs_swir_1": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/FS/BAND-6",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_fs_swir_2": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/FS/BAND-7",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_fv_blue": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/FV/BAND-2",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_fv_coastal_aerosol": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/FV/BAND-1",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_fv_green": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/FV/BAND-3",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_fv_nir": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/FV/BAND-5",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_fv_red": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/FV/BAND-4",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_fv_swir_1": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/FV/BAND-6",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_fv_swir_2": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/FV/BAND-7",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_incident": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INCIDENT-ANGLES/INCIDENT-ANGLE",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_latitude": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/LONGITUDE-LATITUDE/LATITUDE",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_longitude": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/LONGITUDE-LATITUDE/LONGITUDE",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_relative_azimuth": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/SATELLITE-SOLAR/RELATIVE-AZIMUTH",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_relative_slope": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/RELATIVE-SLOPE/RELATIVE-SLOPE",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_s_blue": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/S/BAND-2",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_s_coastal_aerosol": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/S/BAND-1",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_s_green": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/S/BAND-3",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_s_nir": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/S/BAND-5",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_s_red": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/S/BAND-4",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_s_swir_1": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/S/BAND-6",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_s_swir_2": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/S/BAND-7",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_satellite_azimuth": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/SATELLITE-SOLAR/SATELLITE-AZIMUTH",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_satellite_view": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/SATELLITE-SOLAR/SATELLITE-VIEW",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_self_shadow": {
                "layer": "//rg1_oa_self_shadow",
                "path": str(
                    tmp_path
                    / "LC80920842016180LGN01/LC80920842016180LGN01.converted.datasets.h5"  # noqa: W503
                ),
            },
            "rg1_oa_slope": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/SLOPE-ASPECT/SLOPE",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_solar_azimuth": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/SATELLITE-SOLAR/SOLAR-AZIMUTH",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_solar_zenith": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/SATELLITE-SOLAR/SOLAR-ZENITH",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_terrain_shadow": {
                "layer": "//rg1_oa_terrain_shadow",
                "path": str(
                    tmp_path
                    / "LC80920842016180LGN01/LC80920842016180LGN01.converted.datasets.h5"  # noqa: W503
                ),
            },
            "rg1_oa_timedelta": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/SATELLITE-SOLAR/TIME-DELTA",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_ts_blue": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/TS/BAND-2",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_ts_coastal_aerosol": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/TS/BAND-1",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_ts_green": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/TS/BAND-3",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_ts_nir": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/TS/BAND-5",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_ts_red": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/TS/BAND-4",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_ts_swir_1": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/TS/BAND-6",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
            "rg1_oa_ts_swir_2": {
                "layer": "//LC80920842016180LGN01/RES-GROUP-1/INTERPOLATED-ATMOSPHERIC-COEFFICIENTS/TS/BAND-7",
                "path": str(
                    tmp_path / "LC80920842016180LGN01/LC80920842016180LGN01.wagl.h5"
                ),
            },
        },
        "product": {
            "href": "https://collections.dea.ga.gov.au/product/ga_ls8c_ard_3",
            "name": "ga_ls8c_ard_3",
        },
        "properties": {
            "datetime": datetime(2016, 6, 28, 0, 2, 28, 624635, tzinfo=timezone.utc),
            "eo:cloud_cover": 63.069613577531236,
            "eo:gsd": 30.0,
            "eo:instrument": "OLI_TIRS",
            "eo:platform": "landsat-8",
            "eo:sun_azimuth": 33.65512534,
            "eo:sun_elevation": 23.98836172,
            "fmask:clear": 32.735343657403305,
            "fmask:cloud": 63.069613577531236,
            "fmask:cloud_shadow": 4.139470857647722,
            "fmask:snow": 0.005053323801138007,
            "fmask:water": 0.050518583616596675,
            "gqa:abs_iterative_mean_x": 0.21,
            "gqa:abs_iterative_mean_xy": 0.27,
            "gqa:abs_iterative_mean_y": 0.18,
            "gqa:abs_x": 0.3,
            "gqa:abs_xy": 0.39,
            "gqa:abs_y": 0.25,
            "gqa:cep90": 0.46,
            "gqa:iterative_mean_x": -0.17,
            "gqa:iterative_mean_xy": 0.21,
            "gqa:iterative_mean_y": 0.12,
            "gqa:iterative_stddev_x": 0.19,
            "gqa:iterative_stddev_xy": 0.25,
            "gqa:iterative_stddev_y": 0.17,
            "gqa:mean_x": -0.1,
            "gqa:mean_xy": 0.14,
            "gqa:mean_y": 0.1,
            "gqa:stddev_x": 0.35,
            "gqa:stddev_xy": 0.45,
            "gqa:stddev_y": 0.29,
            "landsat:collection_category": "T1",
            "landsat:collection_number": 1,
            "landsat:landsat_product_id": "LC08_L1TP_092084_20160628_20170323_01_T1",
            "landsat:landsat_scene_id": "LC80920842016180LGN01",
            "landsat:wrs_path": 92,
            "landsat:wrs_row": 84,
            "odc:dataset_version": "3.1.0",
            "odc:file_format": "HDF5",
            "odc:processing_datetime": datetime(
                2019, 7, 11, 23, 29, 29, 21245, tzinfo=timezone.utc
            ),
            "odc:producer": "ga.gov.au",
            "odc:product_family": "ard",
            "odc:region_code": "092084",
        },
    }


def test_package(tmp_path, expected_wagl_output: dict):
    for eods_granule in Granule.for_path(
        WAGL_INPUT_PATH,
        granule_names,
        L1_METADATA_PATH,
        FMASK_IMAGE_PATH,
        FMASK_DOC_PATH,
        GQA_DOC_PATH,
        TESP_DOC_PATH,
    ):
        ds_id, md_path = package_non_standard(tmp_path, eods_granule)
        with md_path.open("r") as f:
            generated_doc = yaml.safe_load(f)
            del generated_doc["id"]
        # Assert md_path matches expected_wagl_output
        # assert expected_wagl_output == yaml.load(md_path.open('r'))
        assert expected_wagl_output == generated_doc
