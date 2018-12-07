#!/usr/bin/env python

"""
A temporary workflow for processing S2 data into an ARD package.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from os.path import join as pjoin, basename
from posixpath import join as ppjoin
import shutil
import re
import logging
import traceback
from urllib.parse import urlencode

from structlog import wrap_logger
from structlog.processors import JSONRenderer

import luigi
from luigi.local_target import LocalFileSystem

from wagl.acquisition import preliminary_acquisitions_data
from wagl.singlefile_workflow import DataStandardisation
from tesp.package import package, PATTERN2, ARD
from tesp.constants import ProductPackage

from eugl.fmask import fmask
from eugl.gqa import GQATask


ERROR_LOGGER = wrap_logger(logging.getLogger('errors'),
                           processors=[JSONRenderer(indent=1, sort_keys=True)])
STATUS_LOGGER = wrap_logger(logging.getLogger('status'),
                            processors=[JSONRenderer(indent=1, sort_keys=True)])
INTERFACE_LOGGER = logging.getLogger('luigi-interface')


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
    acq_parser_hint = luigi.OptionalParameter(default='')

    def requires(self):
        # for the time being have fmask require wagl,
        # no point in running fmask if wagl fails...
        # return WorkDir(self.level1, dirname(self.workdir))
        return DataStandardisation(self.level1, self.workdir, self.granule)

    def output(self):
        out_fname = pjoin(self.workdir, '{}.fmask.img'.format(self.granule))

        return luigi.LocalTarget(out_fname)

    def run(self):
        with self.output().temporary_path() as out_fname:
            fmask(self.level1, self.granule, out_fname, self.workdir,
                  self.acq_parser_hint)


# useful for testing fmask via the CLI
class Fmask(luigi.WrapperTask):

    """
    A helper task that issues RunFmask Tasks.
    """

    level1 = luigi.Parameter()
    workdir = luigi.Parameter()
    acq_parser_hint = luigi.OptionalParameter(default='')

    def requires(self):
        # issues task per granule
        for granule in preliminary_acquisitions_data(self.level1, self.acq_parser_hint):
            yield RunFmask(self.level1, granule['id'], self.workdir)


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

    def requires(self):
        # Ensure configuration values are valid
        self._validate_cfg()

        tasks = {'wagl': DataStandardisation(self.level1, self.workdir,
                                             self.granule),
                 'fmask': RunFmask(self.level1, self.granule, self.workdir),
                 'gqa': GQATask(self.level1, self.acq_parser_hint, self.granule, self.workdir)}

        return tasks

    def output(self):
        granule = re.sub(PATTERN2, ARD, self.granule)
        out_fname = pjoin(self.pkgdir, granule, 'CHECKSUM.sha1')

        return luigi.LocalTarget(out_fname)

    def run(self):
        inputs = self.input()

        package(self.level1, inputs['wagl'].path, inputs['fmask'].path,
                inputs['gqa'].path, self.yamls_dir, self.pkgdir, self.granule,
                self.products, self.acq_parser_hint)

        if self.cleanup:
            shutil.rmtree(self.workdir)

    def _validate_cfg(self):
        assert ProductPackage.validate_products(self.products)


def list_packages(workdir, acq_parser_hint, pkgdir):
    def worker(level1):
        work_root = pjoin(workdir, '{}.ARD'.format(basename(level1)))

        result = []
        for granule in preliminary_acquisitions_data(level1, acq_parser_hint):
            work_dir = pjoin(work_root, granule['id'])
            ymd = granule['datetime'].strftime('%Y-%m-%d')
            outdir = pjoin(pkgdir, ymd)
            result.append(Package(level1, work_dir, granule['id'], outdir))

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
            for package in future.result():
                yield package


if __name__ == '__main__':
    luigi.run()
