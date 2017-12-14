# coding=utf-8
"""
Execution method for FMask - http://pythonfmask.org - (cloud, cloud shadow, water and
snow/ice classification) code supporting Sentinel-2 Level 1 C SAFE format zip archives hosted by the
Australian Copernicus Data Hub - http://www.copernicus.gov.au/ - for direct (zip) read access
by datacube.

example usage:
    fmask_cophub.py S2A_MSIL1C_20170104T052712_N0204_R019_T43MDR_20170104T052713.zip
    --output /tmp/
"""
from __future__ import absolute_import
import os
import tempfile
import logging
from xml.etree import ElementTree
from pathlib import Path
import zipfile
from collections import OrderedDict
import click
os.environ["CPL_ZIP_ENCODING"] = "UTF-8"


def prepare_dataset(path):
    """
    Returns a dictionary of image paths, granue id and metadata file location for the granules
    contained within the input file
    """
    tasks = []
    if path.suffix == '.zip':
        zipfile.ZipFile(str(path))
        z = zipfile.ZipFile(str(path))
        xmlzipfiles = [s for s in z.namelist() if "MTD_MSIL1C.xml" in s]
        if xmlzipfiles == []:
            pattern = str(path.name)
            pattern = pattern.replace('PRD_MSIL1C', 'MTD_SAFL1C')
            pattern = pattern.replace('.zip', '.xml')
            xmlzipfiles = [s for s in z.namelist() if pattern in s]
        mtd_xml = z.read(xmlzipfiles[0])
        root = ElementTree.XML(mtd_xml)

    else:
        root = ElementTree.parse(str(path)).getroot()
    processing_baseline = root.findall('./*/Product_Info/PROCESSING_BASELINE')[0].text
    single_granule_archive = False
    granules = {granule.get('granuleIdentifier'): [imid.text for imid in granule.findall('IMAGE_ID')]
                for granule in root.findall('./*/Product_Info/Product_Organisation/Granule_List/Granules')}
    if not granules:
        single_granule_archive = True
        granules = {granule.get('granuleIdentifier'): [imid.text for imid in granule.findall('IMAGE_FILE')]
                    for granule in root.findall('./*/Product_Info/Product_Organisation/Granule_List/Granule')}
        if not [] in granules.values():
            single_granule_archive = True
        else:
            # the dreaded third variant that looks like a single granule archive but has multiple granules...
            granules = {granule.get('granuleIdentifier'): [imid.text for imid in granule.findall('IMAGE_ID')]
                        for granule in root.findall('./*/Product_Info/Product_Organisation/Granule_List/Granule')}
            single_granule_archive = False
    for granule_id, images in granules.items():
        images_ten_list = []
        images_twenty_list = []
        images_sixty_list = []
        # Not required for Zip method - uses granule metadata
        img_data_path = str(path.parent.joinpath('GRANULE', granule_id, 'IMG_DATA'))
        if not path.suffix == '.zip':
            gran_path = str(path.parent.joinpath('GRANULE', granule_id, granule_id[:-7].replace('MSI', 'MTD') + '.xml'))
            root = ElementTree.parse(gran_path).getroot()
        else:
            xmlzipfiles = [s for s in z.namelist() if 'MTD_TL.xml' in s]
            if xmlzipfiles == []:
                pattern = granule_id.replace('MSI', 'MTD')
                pattern = pattern.replace('_N'+processing_baseline, '.xml')
                xmlzipfiles = [s for s in z.namelist() if pattern in s]
            mtd_xml = z.read(xmlzipfiles[0])
            root = ElementTree.XML(mtd_xml)
            img_data_path = str(path)+'!'
            img_data_path = 'zip:'+img_data_path+str(z.namelist()[0])
            #for earlier versions of zip archive - use GRANULES
            if single_granule_archive is False:
                img_data_path = img_data_path+str(Path('GRANULE').joinpath(granule_id, 'IMG_DATA'))
        # Add the QA band
        qi_band = root.findall('./*/PVI_FILENAME')[0].text
        qi_band = qi_band.replace('.jp2', '')
        images.append(qi_band)
        for image in images:
            ten_list = ['B02', 'B03', 'B04', 'B08']
            twenty_list = ['B05', 'B06', 'B07', 'B11', 'B12', 'B8A']
            sixty_list = ['B01', 'B09', 'B10']
            for item in ten_list:
                if item in image:
                    images_ten_list.append(os.path.join(img_data_path, image + ".jp2"))
            for item in twenty_list:
                if item in image:
                    images_twenty_list.append(os.path.join(img_data_path, image + ".jp2"))
            for item in sixty_list:
                if item in image:
                    images_sixty_list.append(os.path.join(img_data_path, image + ".jp2"))
        img_dict = OrderedDict([('B01', ''), ('B02', ''), ('B03', ''), ('B04', ''), ('B05', ''), ('B06', ''), ('B07', ''), ('B08', ''), ('B8A', ''), ('B09', ''), ('B10', ''), ('B11', ''), ('B12', '')])
        for image in images:
            if image[-3:] in img_dict.keys():
                img_path = os.path.join(img_data_path, image + ".jp2")
                band_label = image[-3:]
            img_dict[band_label] = {'path': img_path, 'layer': 1}
        tasks.append((img_dict, granule_id, xmlzipfiles[0]))
    return tasks


def fmask(dataset_path, task, outdir):
    """
    Execute the fmask process.
    """
    img_dict, granule_id, mtd_xml = task
    with tempfile.TemporaryDirectory(dir=outdir,
                                     prefix='pythonfmask-') as tmpdir:
        # filenames
        vrt_fname = os.path.join(tmpdir, granule_id + ".vrt")
        angles_fname = os.path.join(tmpdir, granule_id + ".angles.img")
        cloud_fname = os.path.join(outdir, granule_id + ".cloud.img")

        # zipfile extraction
        zipfile_path = os.path.join(tmpdir, Path(mtd_xml).name)
        logging.info("Unzipping "+mtd_xml)
        os.system("unzip -p " + str(dataset_path) + " " + mtd_xml + " > " + zipfile_path)

        # vrt creation
        command = ["gdalbuildvrt", "-resolution", "user", "-tr", "20", "20",
                   "-separate", "-overwrite", vrt_fname]
        for key in img_dict.keys():
            command.append(" "+img_dict[key]['path'].replace('zip:', '/vsizip/').replace('!', "/"))

        command_str = ' '.join(command)
        logging.info("Create  VRT " + vrt_fname)
        os.system(command_str)

        # angles generation
        command = "fmask_sentinel2makeAnglesImage.py -i " + zipfile_path + " -o " + angles_fname
        logging.info("Create angle file " + angles_fname)
        os.system(command)

        # run fmask
        command = "fmask_sentinel2Stacked.py -a " + vrt_fname + " -z " + angles_fname + " -o " + cloud_fname
        logging.info("Create fmask output " + cloud_fname)
        os.system(command)


@click.command(help=__doc__)
@click.option('--output', help="Write datasets into this directory",
              type=click.Path(exists=False, writable=True, dir_okay=True))
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)


def main(output, datasets):
    """
    For each dataset in input 'datasets' generate FMask and Contiguity
    outputs and write to the destination path specified by 'output'
    """
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
    for dataset in datasets:
        outpath = os.path.join(os.path.abspath(output), dataset + '.fmask')
        if not os.path.exists(outpath):
            os.makedirs(outpath)

        path = Path(dataset)
        tasks = prepare_dataset(path)
        for i in tasks:
            fmask(path, i, outpath)

            # TODO: proper final output location
            # command = ["gdal_translate", "-of", "GTiff", "-co", "COMPRESS=DEFLATE",
            #            "-co", "ZLEVEL=4", "-co", "PREDICTOR=2",
            #            "-co", "COPY_SRC_OVERVIEWS=YES",
            #            cloud, out+".FMASK_QA.TIF"]

if __name__ == "__main__":
    main()
