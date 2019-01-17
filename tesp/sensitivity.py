from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import re
import csv
import shutil
from os.path import join as pjoin, basename, exists

import luigi
import h5py
import numpy
import rasterio
import yaml

from wagl.acquisition import preliminary_acquisitions_data
from wagl.singlefile_workflow import DataStandardisation
from wagl.constants import ArdProducts, GroupName, DatasetName
from tesp.package import PATTERN2, ARD
from tesp.workflow import RunFmask


class ExperimentList(luigi.WrapperTask):
    """
    A helper task that issues `Experiment` tasks for each Level-1
    dataset listed in the `level1_list` parameter and each experiment specified.
    """
    level1_list = luigi.Parameter()
    workdir = luigi.Parameter()
    pkgdir = luigi.Parameter()
    experiment_list_yaml = luigi.Parameter()
    tags = luigi.ListParameter(default=[])
    cleanup = luigi.BoolParameter()
    acq_parser_hint = luigi.OptionalParameter(default='')

    def requires(self):
        with open(self.level1_list) as src:
            level1_list = [level1.strip() for level1 in src.readlines()]

        with open(self.experiment_list_yaml) as fl:
            experiments = yaml.load(fl)

        def worker(level1):
            for granule in preliminary_acquisitions_data(level1, self.acq_parser_hint):
                for tag, settings in experiments.items():
                    if not self.tags or tag in self.tags:
                        work_root = pjoin(self.workdir, '{}.{}'.format(basename(level1), tag))
                        work_dir = pjoin(work_root, granule['id'])

                        yield Experiment(level1, work_dir, granule['id'], self.pkgdir, tag, settings, self.cleanup)

        executor = ThreadPoolExecutor()
        futures = [executor.submit(worker, level1) for level1 in level1_list]

        for future in as_completed(futures):
            for exp in future.result():
                yield exp

# NOTE we probably need one more level here that creates the temporal mean image

class Experiment(luigi.Task):
    """ Sensitivity analysis experiment. """
    level1 = luigi.Parameter()
    workdir = luigi.Parameter()
    granule = luigi.OptionalParameter(default='')
    pkgdir = luigi.Parameter()
    tag = luigi.Parameter()
    settings = luigi.DictParameter()
    cleanup = luigi.BoolParameter()

    def _output_folder(self):
        granule = re.sub(PATTERN2, ARD, self.granule)
        return pjoin(self.pkgdir, self.tag, granule)

    def _output_filename(self):
        return pjoin(self._output_folder(), f'summary.csv')

    def requires(self):
        settings = {}
        for key, value in self.settings.items():
            if key == 'normalized_solar_zenith':
                settings[key] = value
            else:
                settings[key] = {'user': value}

        tasks = {
            'wagl': DataStandardisation(self.level1, self.workdir, self.granule, **settings),
            'fmask': RunFmask(self.level1, self.granule, self.workdir, upstream_settings=settings)
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

            for entry in experiment_summary(inputs['wagl'].path, inputs['fmask'].path,
                                            self.granule, self.tag, self.settings):
                writer.writerow(entry)

        if self.cleanup:
            shutil.rmtree(self.workdir)


def experiment_summary(l2_path, fmask_path, granule, tag, settings):

    with rasterio.open(fmask_path) as mask_file:
        fmask = mask_file.read(1)

    valid_pixels = fmask == 1

    def mask_invalid(img):
        return numpy.where(img != -999, img, numpy.nan)

    yield ['product', 'date', 'granule', 'band', 'experiment', 'mean', 'std', 'valid_pixels']

    with h5py.File(l2_path) as h5:

        dataset = h5[granule]
        date = dataset[GroupName.ATMOSPHERIC_INPUTS_GRP.value].attrs['acquisition-datetime'][:len('2000-01-01')]

        def process(group):
            for product in group:
                for band in group[product]:
                    ds = group[product][band]

                    band_name = ds.attrs['alias']
                    data = numpy.where(valid_pixels, mask_invalid(ds[:]), numpy.nan)

                    yield [product, date, granule, band_name,
                           tag, numpy.nanmean(data), numpy.nanstd(data), numpy.sum(valid_pixels)]

        for res_group in dataset:
            if res_group.startswith('RES-GROUP'):
                yield from process(dataset[res_group][GroupName.STANDARD_GROUP.value]['REFLECTANCE'])
