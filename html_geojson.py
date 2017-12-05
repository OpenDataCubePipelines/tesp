# coding=utf-8
"""
Execution method for contiguous observations within band stack
example usage:
    contiguity.py <allbands.vrt>
    --output /tmp/
"""
from __future__ import absolute_import
import os
import logging
import rasterio

import folium
from folium import GeoJson
import rasterio.features
import shapely.affinity
import shapely.geometry
import shapely.ops
import shapely.geometry
import shapely
import geopandas as gpd
import json
import click
os.environ["CPL_ZIP_ENCODING"] = "UTF-8"

def valid_region(fname, mask_value=None):
    """
    Return valid data region for input images based on mask value and input image path
    """
    mask = None
    logging.info("Valid regions for %s", fname)
    # ensure formats match
    with rasterio.open(str(fname), 'r') as dataset:
        transform = dataset.transform
        img = dataset.read(1)
        if mask_value is not None:
            new_mask = img & mask_value == mask_value
        else:
            new_mask = img != 0
        if mask is None:
            mask = new_mask
        else:
            mask |= new_mask
    shapes = rasterio.features.shapes(mask.astype('uint8'), mask=mask)
    shape = shapely.ops.unary_union([shapely.geometry.shape(shape) for shape, val in shapes if val == 1])
    type(shapes)
    # convex hull
    geom = shape.convex_hull
    # buffer by 1 pixel
    geom = geom.buffer(1, join_style=3, cap_style=3)
    # simplify with 1 pixel radius
    geom = geom.simplify(1)
    # intersect with image bounding box
    geom = geom.intersection(shapely.geometry.box(0, 0, mask.shape[1], mask.shape[0]))
    # transform from pixel space into CRS space
    geom = shapely.affinity.affine_transform(geom, (transform[1], transform[2], transform[4], transform[5], transform[0], transform[3]))    
    return geom

@click.command(help=__doc__)
@click.argument('contiguity', 
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)

def main(contiguity):
    """
    For input contiguity write geojson valid data extent and publish html folium map to 
    'contiguity directory'
    """
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
    print(contiguity)
    contiguity = os.path.abspath(str(contiguity[0]))
    out_dir = os.path.dirname(contiguity)
    geo_path = os.path.join(out_dir, 'valid_data.geojson')
    html_path = os.path.join(out_dir, 'index.html')
    try:
        os.remove(html_path)
        os.remove(geo_path)
    except OSError:
        pass
    geom = valid_region(contiguity)
    logging.info("Create valid bounds " + geo_path)
    gpdsr = gpd.GeoSeries([geom])
    dataset = rasterio.open(contiguity, 'r') 
    gpdsr.crs = dataset.crs.to_dict()   
    dataset.close()
    del dataset
    gpdsr = gpdsr.to_crs({'init': 'epsg:4326'})
    gpdsr.to_file(geo_path, driver='GeoJSON')

    with open(geo_path) as f:
        geojson = json.load(f)
    m = folium.Map()
    GeoJson(geojson).add_to(m)
    m.fit_bounds(GeoJson(geojson).get_bounds())
    m.save(html_path)
    
if __name__ == "__main__":
    main()
    