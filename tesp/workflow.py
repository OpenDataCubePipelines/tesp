#!/usr/bin/env python

"""
A temporary workflow for processing S2 data into an ARD package.
"""

from os.path import join as pjoin, basename
from pathlib import Path
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
from luigi.util import inherits

from luigi.contrib.s3 import S3FlagTarget, S3Client

# Required for the ...S3 Workflow
import boto3
import boto.s3.connection

from wagl.acquisition import acquisitions
from wagl.singlefile_workflow import DataStandardisation
from tesp.package import package, PATTERN2, ARD
from tesp.constants import ProductPackage

from eugl.fmask import fmask


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
    acq_parser_hint = luigi.Parameter(default=None)

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
    acq_parser_hint = luigi.Parameter(default=None)

    def requires(self):
        # issues task per granule
        container = acquisitions(self.level1, self.acq_parser_hint)
        for granule in container.granules:
            yield RunFmask(self.level1, granule, self.workdir)


# TODO: GQA implementation
# class Gqa(luigi.Task):

#     level1 = luigi.Parameter()
#     outdir = luigi.Parameter()


class Package(luigi.Task):

    """
    Creates the final packaged product once wagl, Fmask
    and gqa have executed successfully.
    """

    level1 = luigi.Parameter()
    workdir = luigi.Parameter()
    granule = luigi.Parameter(default=None)
    pkgdir = luigi.Parameter()
    yamls_dir = luigi.Parameter()
    cleanup = luigi.BoolParameter()
    acq_parser_hint = luigi.Parameter(default=None)
    products = luigi.ListParameter(default=ProductPackage.default())

    def requires(self):
        # Ensure configuration values are valid
        self._validate_cfg()

        tasks = {'wagl': DataStandardisation(self.level1, self.workdir,
                                             self.granule),
                 'fmask': RunFmask(self.level1, self.granule, self.workdir)}
        # TODO: GQA implementation
        # 'gqa': Gqa()}

        return tasks

    def output(self):
        granule = re.sub(PATTERN2, ARD, self.granule)
        out_fname = pjoin(self.pkgdir, granule, 'CHECKSUM.sha1')

        return luigi.LocalTarget(out_fname)

    def run(self):
        inputs = self.input()
        package(self.level1, inputs['wagl'].path, inputs['fmask'].path,
                self.yamls_dir, self.pkgdir, self.granule,
                self.products, self.acq_parser_hint)

        if self.cleanup:
            shutil.rmtree(self.workdir)

    def _validate_cfg(self):
        assert ProductPackage.validate(self.products)


class ARDP(luigi.WrapperTask):

    """
    A helper Task that issues Package Tasks for each Level-1
    dataset listed in the `level1_list` parameter.
    """

    level1_list = luigi.Parameter()
    workdir = luigi.Parameter()
    pkgdir = luigi.Parameter()
    acq_parser_hint = luigi.Parameter(default=None)

    def requires(self):
        with open(self.level1_list) as src:
            level1_list = [level1.strip() for level1 in src.readlines()]

        for level1 in level1_list:
            work_root = pjoin(self.workdir, '{}.ARD'.format(basename(level1)))
            container = acquisitions(level1, self.acq_parser_hint)
            for granule in container.granules:
                work_dir = container.get_root(work_root, granule=granule)
                acq = container.get_acquisitions(None, granule, False)[0]
                ymd = acq.acquisition_datetime.strftime('%Y-%m-%d')
                pkgdir = pjoin(self.pkgdir, ymd)
                yield Package(level1, work_dir, granule, pkgdir)


@inherits(Package)
class PackageS3(luigi.Task):

    """
    Uploads the packaged data to an s3_bucket and prefix after running the
    Package task successfully.
    s3_bucket and s3_key_prefix are used for the destination path,
    """

    s3_bucket = luigi.Parameter()
    s3_key_prefix = luigi.Parameter()
    s3_object_base_tags = luigi.DictParameter(default={},
                                              significant=False)
    s3_client_args = luigi.DictParameter(default={},
                                         significant=False)

    MEDIA_TYPES = {
        'geojson': 'application/geo+json',
        'htm': 'text/html',
        'html': 'text/html',
        'jpg': 'image/jpeg',
        'sha1': 'text/plain',
        'tif': 'image/tiff',
        'tiff': 'image/tiff',
        'vrt': 'text/xml',
        'xml': 'text/xml',
        'yaml': 'text/plain',
        'yml': 'text/plain',
        'md': 'text/plain',
    }

    def get_s3_client(self):
        s3_client = S3Client()

        if 'aws_role_arn' in self.s3_client_args:
            s3_client.s3 = self._boto3_refresh_authentication()

        return s3_client

    def _boto3_refresh_authentication(self):
        """
        Role credentials are fetched via boto3 to enable session token
        configuration for authentication
        """
        s3_connection_args = {}

        if 'aws_role_arn' in self.s3_client_args:
            sts = boto3.client('sts')
            credentials = sts.assume_role(
                RoleArn=self.s3_client_args['aws_role_arn'],
                RoleSessionName=self.s3_client_args.get(
                    'aws_role_session_name', 'tesp_packaging'
                )
            )

            s3_connection_args.update({
                'aws_access_key_id': credentials['Credentials']['AccessKeyId'],
                'aws_secret_access_key': credentials['Credentials']['SecretAccessKey'],
                'security_token': credentials['Credentials']['SessionToken']
            })

        return boto.s3.connection.S3Connection(**s3_connection_args)


    def requires(self):
        return Package(self.level1, self.workdir, self.granule, self.pkgdir)

    def output(self):
        # Assumes that the flag file is at the root of the package
        resolved_checksum = Path(self.input().path).resolve()
        target_prefix = 's3://{}/{}/{}/'.format(
            self.s3_bucket, self.s3_key_prefix, resolved_checksum.parent.name
        )

        STATUS_LOGGER.info(
            task='check-s3-flag',
            path=target_prefix,
            flag=resolved_checksum.name
        )

        return S3FlagTarget(
            path=target_prefix,
            client=self.get_s3_client(),
            flag=resolved_checksum.name
        )

    def run(self):
        granule = Path(self.input().path).resolve().parent
        granule_prefix_len = len(granule.parent.as_posix())
        target_prefix = 's3://{}/{}'.format(self.s3_bucket, self.s3_key_prefix)
        s3_client = self.get_s3_client()

        for path in granule.rglob('*'):
            if path.is_dir():
                continue

            target_path = path.as_posix()[granule_prefix_len:]  # path including granule_id
            headers = {
                'Content-Type': self._get_content_mediatype(path),
                'x-amz-tagging': urlencode(self.s3_object_base_tags)
            }

            STATUS_LOGGER.info(
                task='upload-file',
                local_path=path,
                destination_s3_path=target_prefix + target_path,
                headers=headers
            )

            s3_client.put_multipart(
                local_path=path,
                destination_s3_path=target_prefix + target_path,
                headers=headers
            )

    @classmethod
    def _get_content_mediatype(cls, path):
        # Special case, README files don't have a suffix
        if path.stem == 'README':
            return 'text/plain'

        return cls.MEDIA_TYPES.get(
            str(path.suffix)[1:].lower(),
            'application/octet-stream'
        )


@inherits(ARDP)
class ARDPS3(luigi.WrapperTask):

    """
    A helper Task that issues PackageS3 tasks for each Level-1
    dataset listed in the `level1_list` parameter.
    """

    s3_bucket = luigi.Parameter()
    s3_key_prefix = luigi.Parameter()

    def requires(self):
        with open(self.level1_list) as src:
            level1_list = [level1.strip() for level1 in src.readlines()]

        for level1 in level1_list:
            work_root = pjoin(self.workdir, '{}.ARD'.format(basename(level1)))
            container = acquisitions(level1, self.acq_parser_hint)
            for granule in container.granules:
                work_dir = container.get_root(work_root, granule=granule)
                acq = container.get_acquisitions(None, granule, False)[0]
                ymd = acq.acquisition_datetime.strftime('%Y-%m-%d')
                pkgdir = pjoin(self.pkgdir, ymd)
                yield PackageS3(
                    level1, work_dir, granule, pkgdir, s3_bucket=self.s3_bucket,
                    s3_key_prefix=ppjoin(self.s3_key_prefix, ymd)
                )


if __name__ == '__main__':
    luigi.run()
