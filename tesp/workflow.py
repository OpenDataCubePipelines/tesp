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
import yaml

import luigi
from luigi.local_target import LocalFileSystem

from eodatasets3.wagl import package, Granule

from wagl.acquisition import acquisitions, preliminary_acquisitions_data
from wagl.singlefile_workflow import DataStandardisation
from wagl.logs import TASK_LOGGER, STATUS_LOGGER

from tesp.constants import ProductPackage
from tesp.metadata import _get_tesp_metadata
from tesp.package import package_non_standard

from eugl.fmask import fmask
from eugl import s2cl
from eugl.gqa import GQATask


QA_PRODUCTS = ["gqa", "fmask", "s2cloudless"]


@luigi.Task.event_handler(luigi.Event.FAILURE)
def on_failure(task, exception):
    """Capture any Task Failure here."""
    TASK_LOGGER.exception(
        event="task-failure",
        task=task.get_task_family(),
        params=task.to_str_params(),
        level1=getattr(task, "level1", ""),
        granule=getattr(task, "granule", ""),
        stack_info=True,
        status="failure",
        exception=exception.__str__(),
        traceback=traceback.format_exc().splitlines(),
    )


@luigi.Task.event_handler(luigi.Event.SUCCESS)
def on_success(task):
    """Capture any Task Success here."""
    TASK_LOGGER.info(
        event="task-success",
        task=task.get_task_family(),
        params=task.to_str_params(),
        level1=getattr(task, "level1", ""),
        granule=getattr(task, "granule", ""),
        status="success",
    )


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


class RunS2Cloudless(luigi.Task):
    """
    Execute the s2cloudless algorithm for a given granule.
    """

    level1 = luigi.Parameter()
    granule = luigi.Parameter()
    workdir = luigi.Parameter()
    acq_parser_hint = luigi.OptionalParameter(default="")
    threshold = luigi.FloatParameter(default=s2cl.THRESHOLD)
    average_over = luigi.IntParameter(default=s2cl.AVERAGE_OVER)
    dilation_size = luigi.IntParameter(default=s2cl.DILATION_SIZE)

    def platform_id(self):
        container = acquisitions(self.level1, acq_parser_hint=self.acq_parser_hint)
        sample_acq = container.get_all_acquisitions()[0]
        return sample_acq.platform_id

    def output(self):
        if not self.platform_id().startswith("SENTINEL"):
            return None

        prob_out_fname = pjoin(
            self.workdir, "{}.prob.s2cloudless.tif".format(self.granule)
        )
        mask_out_fname = pjoin(
            self.workdir, "{}.mask.s2cloudless.tif".format(self.granule)
        )
        metadata_out_fname = pjoin(
            self.workdir, "{}.s2cloudless.yaml".format(self.granule)
        )

        out_fnames = {
            "cloud_prob": luigi.LocalTarget(prob_out_fname),
            "cloud_mask": luigi.LocalTarget(mask_out_fname),
            "metadata": luigi.LocalTarget(metadata_out_fname),
        }

        return out_fnames

    def run(self):
        if not self.platform_id().startswith("SENTINEL"):
            return

        out_fnames = self.output()
        with out_fnames["cloud_prob"].temporary_path() as prob_out_fname:
            with out_fnames["cloud_mask"].temporary_path() as mask_out_fname:
                with out_fnames["metadata"].temporary_path() as metadata_out_fname:
                    s2cl.s2cloudless_processing(
                        self.level1,
                        self.granule,
                        prob_out_fname,
                        mask_out_fname,
                        metadata_out_fname,
                        self.workdir,
                        acq_parser_hint=self.acq_parser_hint,
                        threshold=self.threshold,
                        average_over=self.average_over,
                        dilation_size=self.dilation_size,
                    )


class S2Cloudless(luigi.Task):
    """
    Execute the Fmask algorithm for a given granule.
    """

    level1 = luigi.Parameter()
    workdir = luigi.Parameter()
    acq_parser_hint = luigi.OptionalParameter(default="")
    threshold = luigi.FloatParameter(default=s2cl.THRESHOLD)
    average_over = luigi.IntParameter(default=s2cl.AVERAGE_OVER)
    dilation_size = luigi.IntParameter(default=s2cl.DILATION_SIZE)

    def requires(self):
        # issues task per granule
        for granule in preliminary_acquisitions_data(self.level1, self.acq_parser_hint):
            yield RunS2Cloudless(
                self.level1,
                granule["id"],
                self.workdir,
                acq_parser_hint=self.acq_parser_hint,
                threshold=self.threshold,
                average_over=self.average_over,
                dilation_size=self.dilation_size,
            )


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
    acq_parser_hint = luigi.OptionalParameter(default="")

    def output(self):
        out_fname1 = pjoin(self.workdir, "{}.fmask.img".format(self.granule))
        out_fname2 = pjoin(self.workdir, "{}.fmask.yaml".format(self.granule))

        out_fnames = {
            "image": luigi.LocalTarget(out_fname1),
            "metadata": luigi.LocalTarget(out_fname2),
        }

        return out_fnames

    def run(self):
        out_fnames = self.output()
        with out_fnames["image"].temporary_path() as out_fname1:
            with out_fnames["metadata"].temporary_path() as out_fname2:
                fmask(
                    self.level1,
                    self.granule,
                    out_fname1,
                    out_fname2,
                    self.workdir,
                    self.acq_parser_hint,
                    self.cloud_buffer_distance,
                    self.cloud_shadow_buffer_distance,
                    self.parallax_test,
                )


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
    acq_parser_hint = luigi.OptionalParameter(default="")

    def requires(self):
        # issues task per granule
        for granule in preliminary_acquisitions_data(self.level1, self.acq_parser_hint):
            yield RunFmask(
                self.level1,
                granule["id"],
                self.workdir,
                self.cloud_buffer_distance,
                self.cloud_shadow_buffer_distance,
                self.parallax_test,
            )


class Package(luigi.Task):

    """
    Creates the final packaged product once wagl, Fmask
    and gqa have executed successfully.
    """

    level1 = luigi.Parameter()
    workdir = luigi.Parameter()
    granule = luigi.OptionalParameter(default="")
    pkgdir = luigi.Parameter()
    yamls_dir = luigi.OptionalParameter(default="")
    cleanup = luigi.BoolParameter()
    acq_parser_hint = luigi.OptionalParameter(default="")
    products = luigi.ListParameter(default=ProductPackage.default())
    qa_products = luigi.ListParameter(default=QA_PRODUCTS)

    # fmask settings
    cloud_buffer_distance = luigi.FloatParameter(default=150.0)
    cloud_shadow_buffer_distance = luigi.FloatParameter(default=300.0)
    parallax_test = luigi.BoolParameter()

    # s2cloudless settings
    threshold = luigi.FloatParameter(default=s2cl.THRESHOLD)
    average_over = luigi.IntParameter(default=s2cl.AVERAGE_OVER)
    dilation_size = luigi.IntParameter(default=s2cl.DILATION_SIZE)

    non_standard_packaging = luigi.BoolParameter()
    product_maturity = luigi.OptionalParameter(default="stable")

    def requires(self):
        # Ensure configuration values are valid
        # self._validate_cfg()

        tasks = {
            "wagl": DataStandardisation(
                self.level1,
                self.workdir,
                self.granule,
                acq_parser_hint=self.acq_parser_hint,
            ),
            "fmask": RunFmask(
                self.level1,
                self.granule,
                self.workdir,
                self.cloud_buffer_distance,
                self.cloud_shadow_buffer_distance,
                self.parallax_test,
                acq_parser_hint=self.acq_parser_hint,
            ),
            "s2cloudless": RunS2Cloudless(
                self.level1,
                self.granule,
                self.workdir,
                acq_parser_hint=self.acq_parser_hint,
                threshold=self.threshold,
                average_over=self.average_over,
                dilation_size=self.dilation_size,
            ),
            "gqa": GQATask(
                level1=self.level1,
                acq_parser_hint=self.acq_parser_hint,
                granule=self.granule,
                workdir=self.workdir,
            ),
        }

        # Need to improve pluggability across tesp/eugl/wagl
        # and adopt patterns that facilitate reuse
        for key in list(tasks.keys()):
            if key != "wagl" and key not in list(self.qa_products):
                del tasks[key]

        return tasks

    def output(self):
        # temp work around. rather than duplicate the packaging logic
        # create a text file to act as a completion target
        # this could be changed to be a database record
        parent_dir = Path(self.workdir).parent
        out_fname = parent_dir.joinpath("{}.completed".format(self.granule))

        return luigi.LocalTarget(str(out_fname))

    def run(self):
        def search_for_external_level1_metadata():
            if self.yamls_dir is None or self.yamls_dir == "":
                return None

            level1_stem = Path(self.level1).stem
            parent_dir = Path(self.level1).parent.name
            return (
                Path(self.yamls_dir) / parent_dir / (level1_stem + ".odc-metadata.yaml")
            )

        # TODO; the package_file func can accept additional fnames for yamls etc
        wagl_fname = Path(self.input()["wagl"].path)
        fmask_img_fname = Path(self.input()["fmask"]["image"].path)
        fmask_doc_fname = Path(self.input()["fmask"]["metadata"].path)
        gqa_doc_fname = Path(self.input()["gqa"].path)

        if self.input()["s2cloudless"] is not None:
            s2cloudless_prob_fname = Path(self.input()["s2cloudless"]["cloud_prob"].path)
            s2cloudless_mask_fname = Path(self.input()["s2cloudless"]["cloud_mask"].path)
            s2cloudless_metadata_fname = Path(self.input()["s2cloudless"]["metadata"].path)
        else:
            s2cloudless_prob_fname = None
            s2cloudless_mask_fname = None
            s2cloudless_metadata_fname = None

        tesp_doc_fname = Path(self.workdir) / "{}.tesp.yaml".format(self.granule)
        with tesp_doc_fname.open("w") as src:
            yaml.safe_dump(_get_tesp_metadata(), src)

        md = {}
        for eods_granule in Granule.for_path(
            wagl_fname,
            granule_names=[self.granule],
            fmask_image_path=fmask_img_fname,
            fmask_doc_path=fmask_doc_fname,
            gqa_doc_path=gqa_doc_fname,
            tesp_doc_path=tesp_doc_fname,
            level1_metadata_path=search_for_external_level1_metadata(),
        ):

            if self.non_standard_packaging:
                ds_id, md_path = package_non_standard(Path(self.pkgdir), eods_granule)
            else:
                ds_id, md_path = package(
                    Path(self.pkgdir),
                    eods_granule,
                    product_maturity=self.product_maturity,
                    included_products=self.products,
                )

            md[ds_id] = md_path
            STATUS_LOGGER.info(
                "packaged dataset",
                granule=self.granule,
                level1=self.level1,
                dataset_id=str(ds_id),
                dataset_path=str(md_path),
            )

        if self.cleanup:
            shutil.rmtree(self.workdir)

        with self.output().temporary_path() as out_fname:
            with open(out_fname, "w") as outf:
                data = {
                    "params": self.to_str_params(),
                    # JSON can't serialise the returned Path obj
                    "packaged_datasets": {str(k): str(v) for k, v in md.items()},
                }
                json.dump(data, outf)


def list_packages(workdir, acq_parser_hint, pkgdir, yamls_dir):
    def worker(level1):
        work_root = pjoin(workdir, "{}.ARD".format(basename(level1)))

        result = []
        for granule in preliminary_acquisitions_data(level1, acq_parser_hint):
            work_dir = pjoin(work_root, granule["id"])
            if yamls_dir is None or yamls_dir == "":
                result.append(
                    Package(
                        level1,
                        work_dir,
                        granule["id"],
                        pkgdir,
                        acq_parser_hint=acq_parser_hint,
                    )
                )
            else:
                result.append(
                    Package(
                        level1,
                        work_dir,
                        granule["id"],
                        pkgdir,
                        acq_parser_hint=acq_parser_hint,
                        yamls_dir=yamls_dir,
                    )
                )

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
    acq_parser_hint = luigi.OptionalParameter(default="")
    yamls_dir = luigi.OptionalParameter(default="")

    def requires(self):
        with open(self.level1_list) as src:
            level1_list = [level1.strip() for level1 in src.readlines()]

        worker = list_packages(
            self.workdir, self.acq_parser_hint, self.pkgdir, self.yamls_dir
        )

        executor = ThreadPoolExecutor()
        futures = [executor.submit(worker, level1) for level1 in level1_list]

        for future in as_completed(futures):
            for _package in future.result():
                yield _package


if __name__ == "__main__":
    luigi.run()
