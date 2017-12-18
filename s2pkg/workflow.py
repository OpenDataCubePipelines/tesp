#!/usr/bin/env python

from os.path import join as pjoin, basename
import luigi
from luigi.local_target import LocalFileSystem

from gaip.singlefile_workflow import DataStandardisation
from s2pkg.fmask_cophub import prepare_dataset, fmask
from s2pkg.package import package


class WorkDir(Luigi.Task):

    level1 = luigi.Parameter()
    outdir = luigi.Parameter()

    def output(self):
        return Luigi.LocalTarget(self.outdir)

    def run(self):
        local_fs = LocalFileSystem()
        local_fs.mkdir(self.output().path)


class RunFmask(Luigi.Task):

    level1 = luigi.Parameter()
    task = luigi.TupleParameter()
    outdir = luigi.Parameter()

    def requires(self):
        return WorkDir(self.level1, self.outdir)

    def output(self):
        out_fname = pjoin(self.outdir, '{}.cloud.img'.format(self.task[0]))

        return luigi.LocalTarget(out_fname)

    def run(self):
        with self.output().temporary_path() as out_fname:
            fmask(self.level1, task, out_fname)


class Fmask(Luigi.WrapperTask):

    level1 = luigi.Parameter()
    outdir = luigi.Parameter()

    def requires(self):
        # issues task per granule
        for task in prepare_dataset(self.level1):
            yield RunFmask(self.level1, task, self.outdir)
        

# TODO: GQA implementation
# class Gqa(Luigi.Task):
# 
#     level1 = luigi.Parameter()
#     outdir = luigi.Parameter()

# class Gaip(Luigi.Task):
# 
#     level1 = luigi.Parameter()
#     outdir = luigi.Parameter()

class Package(Luigi.Task):

    level1 = luigi.Parameter()
    work_dir = luigi.Parameter()
    pkg_dir = luigi.Parameter()
    yamls_dir = luigi.Parameter()

    def requires(self):
        tasks = {'gaip': DataStandardisation(),
                 'fmask': Fmask(self.level1, self.work_dir)}
        # TODO: GQA implementation
        # 'gqa': Gqa()}

        return tasks

    def output(self):
        targets = []
        container = acquisitions(self.level1)
        for granule in container.granules:
            out_fname = pjoin(self.pkg_dir,
                              granule.replace('L1C', 'ARD'),
                              'CHECKSUM.sha1')
            targets.append(Luigi.LocalTarget(out_fname))

        return targets

    def run(self):
        package(self.level1, self.input()['gaip'].path,
                self.input()['fmask'].path, self.yamls_dir, self.work_dir)


class Ard(luigi.WrapperTask):

    level1_list = luigi.Parameter()
    work_dir = luigi.Parameter()
    pkg_dir = luigi.Parameter()

    def requires(self):
        with open(self.level1_list) as src:
            level1_scenes = [scene.strip() for scene in src.readlines()]

        for scene in level1_scenes:
            work_dir = pjoin(self.work_dir, '{}.ARD'.format(basename(scene)))
            yield Package(scene, work_dir, self.pkg_dir)


if __name__ == '__main__':
    luigi.run()
