#!/usr/bin/env python

"""
A temporary workflow for processing S2 data into an ARD package.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from os.path import join as pjoin, basename
from pathlib import Path
import shutil
import traceback
import json

import luigi
from luigi.local_target import LocalFileSystem

from eodatasets3.wagl import package, Granule

from wagl.acquisition import preliminary_acquisitions_data
from wagl.singlefile_workflow import DataStandardisation
from wagl.logs import ERROR_LOGGER

from tesp.constants import ProductPackage

from eugl.fmask import fmask
from eugl.gqa import GQATask


QA_PRODUCTS = ['gqa', 'fmask']


@luigi.Task.event_handler(luigi.Event.FAILURE)
def on_failure(task, exception):
    """Capture any Task Failure here."""
    ERROR_LOGGER.error(task=task.get_task_family(),
                       params=task.to_str_params(),
                       level1=getattr(task, 'level1', ''),
                       exception=exception.__str__(),
                       traceback=traceback.format_exc().splitlines())


class WorkDir(luigi.Task):

    """
    Initialises the working directory in a controlled manner.
    Alternatively this could be initialised upfront during the
    ARD Task submission phase.
    """

    level1 = luigi.Parameter()
    workdir = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.workdir)

    def run(self):
        local_fs = LocalFileSystem()
        local_fs.mkdir(self.output().path)


class RunFmask(luigi.Task):

    """
    Execute the Fmask algorithm for a given granule.
    """

    level1 = luigi.Parameter()
    granule = luigi.Parameter()
    workdir = luigi.Parameter()
    cloud_buffer_distance = luigi.FloatParameter(default=150.0)
    cloud_shadow_buffer_distance = luigi.FloatParameter(default=300.0)
    parallax_test = luigi.BoolParameter()
    upstream_settings = luigi.DictParameter(default={})
    acq_parser_hint = luigi.OptionalParameter(default='')

    def requires(self):
        # for the time being have fmask require wagl,
        # no point in running fmask if wagl fails...
        # return WorkDir(self.level1, dirname(self.workdir))
        return DataStandardisation(
            self.level1, self.workdir, self.granule,
            **self.upstream_settings  # pylint: disable=not-a-mapping
        )

    def output(self):
        out_fname1 = pjoin(self.workdir, '{}.fmask.img'.format(self.granule))
        out_fname2 = pjoin(self.workdir, '{}.fmask.yaml'.format(self.granule))

        out_fnames = {
            'image': luigi.LocalTarget(out_fname1),
            'metadata': luigi.LocalTarget(out_fname2)
        }

        return out_fnames

    def run(self):
        out_fnames = self.output()
        with out_fnames['image'].temporary_path() as out_fname1:
            with out_fnames['metadata'].temporary_path() as out_fname2:
                fmask(self.level1, self.granule, out_fname1, out_fname2,
                      self.workdir, self.acq_parser_hint,
                      self.cloud_buffer_distance,
                      self.cloud_shadow_buffer_distance, self.parallax_test)


# useful for testing fmask via the CLI
class Fmask(luigi.WrapperTask):

    """
    A helper task that issues RunFmask Tasks.
    """

    level1 = luigi.Parameter()
    workdir = luigi.Parameter()
    cloud_buffer_distance = luigi.FloatParameter(default=150.0)
    cloud_shadow_buffer_distance = luigi.FloatParameter(default=300.0)
    parallax_test = luigi.BoolParameter()
    acq_parser_hint = luigi.OptionalParameter(default='')

    def requires(self):
        # issues task per granule
        for granule in preliminary_acquisitions_data(self.level1, self.acq_parser_hint):
            yield RunFmask(self.level1, granule['id'], self.workdir,
                           self.cloud_buffer_distance,
                           self.cloud_shadow_buffer_distance,
                           self.parallax_test)


class Package(luigi.Task):

    """
    Creates the final packaged product once wagl, Fmask
    and gqa have executed successfully.
    """

    level1 = luigi.Parameter()
    workdir = luigi.Parameter()
    granule = luigi.OptionalParameter(default='')
    pkgdir = luigi.Parameter()
    yamls_dir = luigi.OptionalParameter(default='')
    cleanup = luigi.BoolParameter()
    acq_parser_hint = luigi.OptionalParameter(default='')
    products = luigi.ListParameter(default=ProductPackage.default())
    qa_products = luigi.ListParameter(default=QA_PRODUCTS)
    cloud_buffer_distance = luigi.FloatParameter(default=150.0)
    cloud_shadow_buffer_distance = luigi.FloatParameter(default=300.0)
    parallax_test = luigi.BoolParameter()

    def requires(self):
        # Ensure configuration values are valid
        # self._validate_cfg()

        tasks = {'wagl': DataStandardisation(self.level1, self.workdir,
                                             self.granule),
                 'fmask': RunFmask(self.level1, self.granule, self.workdir,
                                   self.cloud_buffer_distance,
                                   self.cloud_shadow_buffer_distance,
                                   self.parallax_test),
                 'gqa': GQATask(self.level1, self.acq_parser_hint, self.granule, self.workdir)}

        # Need to improve pluggability across tesp/eugl/wagl
        # and adopt patterns that facilitate reuse
        for key in list(tasks.keys()):
            if key != 'wagl' and key not in list(self.qa_products):
                del tasks[key]

        return tasks

    def output(self):
        # temp work around. rather than duplicate the packaging logic
        # create a text file to act as a completion target
        # this could be changed to be a database record
        parent_dir = Path(self.workdir).parent
        out_fname = parent_dir.joinpath('{}.completed'.format(self.granule))

        return luigi.LocalTarget(str(out_fname))

    def run(self):
        # TODO; the package_file func can accept additional fnames for yamls etc
        wagl_fname = Path(self.input()['wagl'].path)
        fmask_img_fname = Path(self.input()['fmask']['image'].path)
        fmask_doc_fname = Path(self.input()['fmask']['metadata'].path)
        gqa_doc_fname = Path(self.input()['gqa'].path)

        md = {}
        for eods_granule in Granule.for_path(wagl_fname, [self.granule],
                                             fmask_image_path=fmask_img_fname,
                                             fmask_doc_path=fmask_doc_fname,
                                             gqa_doc_path=gqa_doc_fname):

            ds_id, md_path = package(Path(self.pkgdir),
                                     eods_granule,
                                     self.products)

            md[ds_id] = md_path

        if self.cleanup:
            shutil.rmtree(self.workdir)

        with self.output().temporary_path() as out_fname:
            with open(out_fname, 'w') as outf:
                data = {
                    'params': self.to_str_params(),
                    # JSON can't serialise the returned Path obj
                    'packaged_datasets': {str(k): str(v) for k, v in md.items()},
                }
                json.dump(data, outf)


def list_packages(workdir, acq_parser_hint, pkgdir):
    def worker(level1):
        work_root = pjoin(workdir, '{}.ARD'.format(basename(level1)))

        result = []
        for granule in preliminary_acquisitions_data(level1, acq_parser_hint):
            work_dir = pjoin(work_root, granule['id'])
            result.append(Package(level1, work_dir, granule['id'], pkgdir))

        return result

    return worker


class ARDP(luigi.WrapperTask):

    """
    A helper Task that issues Package Tasks for each Level-1
    dataset listed in the `level1_list` parameter.
    """

    level1_list = luigi.Parameter()
    workdir = luigi.Parameter()
    pkgdir = luigi.Parameter()
    acq_parser_hint = luigi.OptionalParameter(default='')

    def requires(self):
        with open(self.level1_list) as src:
            level1_list = [level1.strip() for level1 in src.readlines()]

        worker = list_packages(self.workdir, self.acq_parser_hint, self.pkgdir)

        executor = ThreadPoolExecutor()
        futures = [executor.submit(worker, level1) for level1 in level1_list]

        for future in as_completed(futures):
            for _package in future.result():
                yield _package


if __name__ == '__main__':
    luigi.run()
