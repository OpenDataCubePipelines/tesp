import os
import re
import csv
import shutil
from os.path import join as pjoin, basename, exists

import luigi
import h5py
import numpy
import rasterio

from wagl.acquisition import acquisitions
from wagl.singlefile_workflow import DataStandardisation
from wagl.constants import ArdProducts, GroupName, DatasetName
from tesp.package import PATTERN2, ARD
from tesp.workflow import RunFmask


def aerosol_summary(l2_path, fmask_path, granule, aerosol):

    with rasterio.open(fmask_path) as mask_file:
        fmask = mask_file.read(1)

    valid_pixels = fmask == 1

    def mask_invalid(img):
        return numpy.where(img != -999, img, numpy.nan)

    yield ['product', 'date', 'granule', 'band', 'aerosol', 'mean', 'std', 'valid_pixels']

    with h5py.File(l2_path) as h5:

        def get(*keys):
            return h5['/'.join([granule, *keys])]

        def band_dataset(product, band):
            for res_group in ['RES-GROUP-2', 'RES-GROUP-1', 'RES-GROUP-0']:
                try:
                    return get(res_group, GroupName.STANDARD_GROUP.value,
                               DatasetName.REFLECTANCE_FMT.value.format(product=product, band_name=band))
                except KeyError:
                    pass

            raise KeyError(f'could not find {product} {band} in {granule}')

        assert abs(aerosol - get(GroupName.ANCILLARY_GROUP.value, DatasetName.AEROSOL.value)[...]) < 0.00001

        date = get(GroupName.ATMOSPHERIC_INPUTS_GRP.value).attrs['acquisition-datetime'][:len('2000-01-01')]

        def process_band(product, band):
            try:
                ds = band_dataset(product, band)
                band_name = ds.attrs['alias']
                data = numpy.where(valid_pixels, mask_invalid(ds[:]), numpy.nan)

                yield [product, date, granule, band_name,
                       aerosol, numpy.nanmean(data), numpy.nanstd(data), numpy.sum(valid_pixels)]
            except KeyError:
                pass

        for product in [p.value for p in ArdProducts]:
            for band in [f'BAND-{b}' for b in range(1, 8)]:
                yield from process_band(product, band)


class Aerosol(luigi.Task):
    """ Sensitivity analysis for aerosol. """
    level1 = luigi.Parameter()
    workdir = luigi.Parameter()
    granule = luigi.OptionalParameter(default='')
    pkgdir = luigi.Parameter()
    aerosol = luigi.FloatParameter(default=0.05)
    cleanup = luigi.BoolParameter()

    def _output_folder(self):
        granule = re.sub(PATTERN2, ARD, self.granule)
        return pjoin(self.pkgdir, granule)

    def _output_filename(self):
        return pjoin(self._output_folder(), f'summary_{self.aerosol}.csv')

    def requires(self):
        tasks = {
            'wagl': DataStandardisation(self.level1, self.workdir, self.granule,
                                        aerosol={'user': self.aerosol}),
            'fmask': RunFmask(self.level1, self.granule, self.workdir)
        }

        return tasks

    def output(self):
        return luigi.LocalTarget(self._output_filename())

    def run(self):
        inputs = self.input()
        outdir = self._output_folder()

        if not exists(outdir):
            os.makedirs(outdir)

        with open(self._output_filename(), 'w+') as csv_file:
            writer = csv.writer(csv_file)

            for entry in aerosol_summary(inputs['wagl'].path, inputs['fmask'].path,
                                         self.granule, self.aerosol):
                writer.writerow(entry)

        if self.cleanup:
            shutil.rmtree(self.workdir)


class Aerosols(luigi.WrapperTask):
    """
    A helper Task that issues Aerosol Tasks for each Level-1
    dataset listed in the `level1_list` parameter.
    """
    level1_list = luigi.Parameter()
    workdir = luigi.Parameter()
    pkgdir = luigi.Parameter()
    aerosols = luigi.ListParameter()
    cleanup = luigi.BoolParameter()
    acq_parser_hint = luigi.OptionalParameter(default='')

    def requires(self):
        with open(self.level1_list) as src:
            level1_list = [level1.strip() for level1 in src.readlines()]

        for level1 in level1_list:
            container = acquisitions(level1, self.acq_parser_hint)

            for aerosol in self.aerosols:
                work_root = pjoin(self.workdir, '{}.AERO{}'.format(basename(level1), str(aerosol)))

                for granule in container.granules:
                    work_dir = container.get_root(work_root, granule=granule)

                    yield Aerosol(level1, work_dir, granule, self.pkgdir, float(aerosol), self.cleanup)
