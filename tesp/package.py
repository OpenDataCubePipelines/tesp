#!/usr/bin/env python
# pylint: disable=too-many-locals

from posixpath import join as ppjoin
from pathlib import Path
import json
import numpy
import h5py
import rasterio
from rasterio.crs import CRS
from affine import Affine
from eodatasets3 import DatasetAssembler, images, utils
import eodatasets3.wagl
from eodatasets3.serialise import loads_yaml, from_path
from eodatasets3.scripts.tostac import dc_to_stac, json_fallback
from boltons.iterutils import get_path
import shutil
import numpy as np
from datacube.utils import jsonify_document

import yaml
from yaml.representer import Representer

from wagl.hdf5 import find


yaml.add_representer(numpy.int8, Representer.represent_int)
yaml.add_representer(numpy.uint8, Representer.represent_int)
yaml.add_representer(numpy.int16, Representer.represent_int)
yaml.add_representer(numpy.uint16, Representer.represent_int)
yaml.add_representer(numpy.int32, Representer.represent_int)
yaml.add_representer(numpy.uint32, Representer.represent_int)
yaml.add_representer(numpy.int, Representer.represent_int)
yaml.add_representer(numpy.int64, Representer.represent_int)
yaml.add_representer(numpy.uint64, Representer.represent_int)
yaml.add_representer(numpy.float, Representer.represent_float)
yaml.add_representer(numpy.float32, Representer.represent_float)
yaml.add_representer(numpy.float64, Representer.represent_float)
yaml.add_representer(numpy.ndarray, Representer.represent_list)


def package_non_standard(outdir, granule):
    """
    yaml creator for the ard pipeline.
    """

    outdir = Path(outdir) / granule.name
    indir = granule.wagl_hdf5.parent

    if indir.is_file():
        shutil.copy(indir, outdir)
    else:
        shutil.copytree(indir, outdir)

    wagl_h5 = outdir / str(granule.name + ".wagl.h5")
    dataset_doc = outdir / str(granule.name + ".yaml")
    boolean_h5 = Path(str(wagl_h5).replace("wagl.h5", "converted.datasets.h5"))
    fmask_img = outdir / str(granule.name + ".fmask.img")

    f = h5py.File(boolean_h5)

    with DatasetAssembler(metadata_path=dataset_doc, naming_conventions="dea") as da:
        level1 = granule.source_level1_metadata
        da.add_source_dataset(level1, auto_inherit_properties=True, inherit_geometry=True)
        da.product_family = "ard"
        da.producer = "ga.gov.au"
        da.properties["odc:file_format"] = "HDF5"

        with h5py.File(wagl_h5, "r") as fid:
            img_paths = [ppjoin(fid.name, pth) for pth in find(fid, "IMAGE")]
            granule_group = fid[granule.name]

            try:
                wagl_path, *ancil_paths = [
                    pth for pth in find(granule_group, "SCALAR") if "METADATA" in pth
                ]
            except ValueError:
                raise ValueError("No nbar metadata found in granule")

            [wagl_doc] = loads_yaml(granule_group[wagl_path][()])

            da.processed = get_path(wagl_doc, ("system_information", "time_processed"))

            platform = da.properties["eo:platform"]
            if platform == "sentinel-2a" or platform == "sentinel-2b":
                org_collection_number = 3
            else:
                org_collection_number = utils.get_collection_number(
                    platform, da.producer, da.properties["landsat:collection_number"]
                )

            da.dataset_version = f"{org_collection_number}.1.0"
            da.region_code = eodatasets3.wagl._extract_reference_code(da, granule.name)

            eodatasets3.wagl._read_gqa_doc(da, granule.gqa_doc)
            eodatasets3.wagl._read_fmask_doc(da, granule.fmask_doc)

            with rasterio.open(fmask_img) as ds:
                fmask_layer = "/{}/OA_FMASK/oa_fmask".format(granule.name)
                data = ds.read(1)
                fmask_ds = f.create_dataset(
                    fmask_layer, data=data, compression="lzf", shuffle=True
                )
                fmask_ds.attrs["crs_wkt"] = ds.crs.wkt
                fmask_ds.attrs["geotransform"] = ds.transform.to_gdal()

                fmask_ds.attrs[
                    "description"
                ] = "Converted from ERDAS Imagine format to HDF5 to work with the limitations of varied formats within ODC"  # noqa E501

                grid_spec = images.GridSpec(
                    shape=ds.shape,
                    transform=ds.transform,
                    crs=CRS.from_wkt(fmask_ds.attrs["crs_wkt"]),
                )

                measurement_name = "oa_fmask"

                pathname = str(outdir.joinpath(boolean_h5))

                no_data = fmask_ds.attrs.get("no_data_value")
                if no_data is None:
                    no_data = float("nan")

                da._measurements.record_image(
                    measurement_name,
                    grid_spec,
                    pathname,
                    fmask_ds[:],
                    layer="/{}".format(fmask_layer),
                    nodata=no_data,
                    expand_valid_data=False,
                )

            for pathname in img_paths:
                ds = fid[pathname]
                ds_path = Path(ds.name)

                # eodatasets internally uses this grid spec to group image datasets
                grid_spec = images.GridSpec(
                    shape=ds.shape,
                    transform=Affine.from_gdal(*ds.attrs["geotransform"]),
                    crs=CRS.from_wkt(ds.attrs["crs_wkt"]),
                )

                # product group name; lambertian, nbar, nbart, oa
                if "STANDARDISED-PRODUCTS" in str(ds_path):
                    product_group = ds_path.parent.name
                elif "INTERPOLATED-ATMOSPHERIC-COEFFICIENTS" in str(ds_path):
                    product_group = "oa_{}".format(ds_path.parent.name)
                else:
                    product_group = "oa"

                # spatial resolution group
                # used to separate measurements with the same name
                resolution_group = "rg{}".format(ds_path.parts[2].split("-")[-1])

                measurement_name = (
                    "_".join(
                        [
                            resolution_group,
                            product_group,
                            ds.attrs.get("alias", ds_path.name),
                        ]
                    )
                    .replace("-", "_")
                    .lower()
                )  # we don't wan't hyphens in odc land

                # include this band in defining the valid data bounds?
                include = True if "nbart" in measurement_name else False

                no_data = ds.attrs.get("no_data_value")
                if no_data is None:
                    no_data = float("nan")

                # if we are of type bool, we'll have to convert just for GDAL
                if ds.dtype.name == "bool":
                    pathname = str(outdir.joinpath(boolean_h5))
                    out_ds = f.create_dataset(
                        measurement_name,
                        data=np.uint8(ds[:]),
                        compression="lzf",
                        shuffle=True,
                        chunks=ds.chunks,
                    )

                    for k, v in ds.attrs.items():
                        out_ds.attrs[k] = v

                    da._measurements.record_image(
                        measurement_name,
                        grid_spec,
                        pathname,
                        out_ds[:],
                        layer="/{}".format(out_ds.name),
                        nodata=no_data,
                        expand_valid_data=include,
                    )
                else:
                    pathname = str(outdir.joinpath(wagl_h5))

                    # work around as note_measurement doesn't allow us to specify the gridspec
                    da._measurements.record_image(
                        measurement_name,
                        grid_spec,
                        pathname,
                        ds[:],
                        layer="/{}".format(ds.name),
                        nodata=no_data,
                        expand_valid_data=include,
                    )

        # the longest part here is generating the valid data bounds vector
        # landsat 7 post SLC-OFF can take a really long time
        return da.done()


def write_stac_metadata(input_metadata, stac_base_url, explorer_base_url):
    dataset = from_path(input_metadata)
    name = input_metadata.stem.replace(".odc-metadata", "")
    output_path = input_metadata.with_name(f"{name}.stac-item.json")

    # Create STAC dict
    item_doc = dc_to_stac(
        dataset,
        input_metadata,
        output_path,
        stac_base_url,
        explorer_base_url,
        do_validate=False,
    )

    with output_path.open("w") as f:
        json.dump(jsonify_document(item_doc), f, indent=4, default=json_fallback)

    return output_path
