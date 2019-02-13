"""
Sensitivity analysis toolset.
"""
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
from affine import Affine

from wagl.acquisition import preliminary_acquisitions_data
from wagl.data import write_img
from wagl.geobox import GriddedGeoBox
from wagl.singlefile_workflow import DataStandardisation
from wagl.constants import GroupName
from tesp.package import PATTERN2, ARD
from tesp.workflow import RunFmask


class ExperimentList(luigi.WrapperTask):
    """
    A helper task that issues `Experiment` tasks for the experiments specified.
    """
    level1_list = luigi.Parameter()
    workdir = luigi.Parameter()
    pkgdir = luigi.Parameter()
    experiment_list_yaml = luigi.Parameter()
    tags = luigi.ListParameter(default=[])
    cleanup = luigi.BoolParameter()
    acq_parser_hint = luigi.OptionalParameter(default='')

    def requires(self):
        with open(self.experiment_list_yaml) as fl:
            experiments = yaml.load(fl)

        for tag, settings in experiments.items():
            # a `tag` is a name given to an experiment
            # the `settings` for an experiment is the set of custom parameter values
            if not self.tags or tag in self.tags:
                yield Experiment(self.level1_list, self.workdir, self.pkgdir,
                                 self.cleanup, self.acq_parser_hint,
                                 tag, settings)

    def run(self):
        if self.cleanup:
            for entry in os.listdir(self.workdir):
                shutil.rmtree(pjoin(self.workdir, entry))


class Experiment(luigi.Task):
    """
    A single sensitivity analysis experiment.
    The `settings` parameter is the set of parameter value overrides.

    Produces statistical summary and mean surface reflectance images
    from a list of level-1 datasets.
    """
    level1_list = luigi.Parameter()
    workdir = luigi.Parameter()
    pkgdir = luigi.Parameter()
    cleanup = luigi.BoolParameter()
    acq_parser_hint = luigi.OptionalParameter(default='')
    tag = luigi.Parameter()
    settings = luigi.DictParameter()

    def requires(self):
        with open(self.level1_list) as src:
            level1_paths = [level1_path.strip() for level1_path in src.readlines()]

        def worker(level1_path):
            for granule in preliminary_acquisitions_data(level1_path, self.acq_parser_hint):
                work_root = pjoin(self.workdir, self.tag, basename(level1_path))
                work_dir = pjoin(work_root, granule['id'])
                yield dict(kind='leaf', level1_path=level1_path, workdir=work_dir, granule=granule['id'])

        # collect file info concurrently since IO is expensive
        executor = ThreadPoolExecutor()
        futures = [executor.submit(worker, level1_path) for level1_path in level1_paths]

        # organize the task list in a tree
        tree = dict_tree([exp for future in as_completed(futures) for exp in future.result()], ['mean_sr'])

        # create a `luigi` task with the dependencies specified by `tree`
        return requires_tree(tree, self)

    def output(self):
        return luigi.LocalTarget(pjoin(self.pkgdir, self.tag, '.done'))

    def run(self):
        outdir = pjoin(self.pkgdir, self.tag, 'mean_sr')
        if not exists(outdir):
            os.makedirs(outdir)

        if self.input():
            unpack(self.input(), outdir)

        with open(pjoin(self.pkgdir, self.tag, '.done'), 'w') as fl:
            pass

        if self.input() and self.cleanup:
            os.remove(self.input()['images'].path)
            if 'fmask' in self.input():
                os.remove(self.input()['fmask'].path)


class MergeImages(luigi.Task):
    """
    Task to merge images in two `.h5` files.
    """
    left = luigi.DictParameter()
    right = luigi.DictParameter()
    prefix = luigi.ListParameter()
    workdir = luigi.Parameter()
    pkgdir = luigi.Parameter()
    tag = luigi.Parameter()
    settings = luigi.DictParameter()
    cleanup = luigi.BoolParameter()

    def requires(self):
        return [requires_tree(self.left, self), requires_tree(self.right, self)]

    @property
    def _target_dir(self):
        return pjoin(self.workdir, self.tag, *self.prefix)

    @property
    def _target(self):
        return pjoin(self._target_dir, 'mean_images.h5')

    def output(self):
        return {'images': luigi.LocalTarget(self._target)}

    def run(self):
        if not exists(self._target_dir):
            os.makedirs(self._target_dir)

        inputs = self.input()
        merge_images(inputs[0], inputs[1], self._target)

        if self.cleanup:
            for dep in inputs:
                os.remove(dep['images'].path)
                if 'fmask' in dep:
                    os.remove(dep['fmask'].path)


class ExperimentGranule(luigi.Task):
    """
    Collect data from a granule.
    Produces a `.csv` file containing the statistics from a `.h5` file from `wagl`.
    """
    level1_path = luigi.Parameter()
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
            'wagl': DataStandardisation(self.level1_path, self.workdir, self.granule, **settings),
            'fmask': RunFmask(self.level1_path, self.granule, self.workdir, upstream_settings=settings)
        }

        return tasks

    def output(self):
        return {'images': self.input()['wagl'],
                'fmask': self.input()['fmask'],
                'csv': luigi.LocalTarget(self._output_filename())}

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


def dict_tree(leaf_list, prefix):
    """
    Given a list of experiment settings `leaf_list`,
    organizes them in a binary tree in order to produce
    the mean surface reflectance image.
    """
    if len(leaf_list) == 0:
        return None

    if len(leaf_list) == 1:
        return leaf_list[0]

    mid = len(leaf_list) // 2
    left = dict_tree(leaf_list[:mid], prefix + ['left'])
    right = dict_tree(leaf_list[mid:], prefix + ['right'])
    return dict(kind='node', prefix=prefix, left=left, right=right)


def requires_tree(tree, parent_task):
    """
    Given a dictionary representation of the experiment task tree,
    creates `luigi` `Task` objects to execute it.
    """
    if tree is None:
        return []

    if tree['kind'] == 'leaf':
        return ExperimentGranule(level1_path=tree['level1_path'], workdir=tree['workdir'], granule=tree['granule'],
                                 pkgdir=parent_task.pkgdir, tag=parent_task.tag, settings=parent_task.settings,
                                 cleanup=parent_task.cleanup)

    return MergeImages(left=tree['left'], right=tree['right'], prefix=tree['prefix'],
                       workdir=parent_task.workdir, pkgdir=parent_task.pkgdir, tag=parent_task.tag,
                       settings=parent_task.settings, cleanup=parent_task.cleanup)


def find_dataset_by_name(product, band_name):
    """
    Look up dataset by the human readable band name
    stored in the `alias` attribute.
    """
    for band in product:
        if product[band].attrs['alias'] == band_name:
            return product[band]

    return None


def unpack(input_target, outdir):
    """
    Convert an `.h5` file to `GeoTIFF` files.
    """
    options = {'blockxsize': 1024, 'blockysize': 1024,
               'compress': 'deflate', 'zlevel': 4}

    input_file, mask = filename_and_mask(input_target)

    def unpack_dataset(product_group, product_name, band):
        dataset = product_group[band]

        # human readable band name
        band_name = dataset.attrs['alias']

        out_file = pjoin(outdir, '{}_{}.tif'.format(product_name, band_name))
        count_file = pjoin(outdir, '{}_{}_valid_pixel_count.tif'.format(product_name, band_name))
        nodata = dataset.attrs.get('no_data_value')
        geobox = GriddedGeoBox.from_dataset(dataset)

        data, count = sum_and_count(product_group, mask, band_name)

        # calculate the mean from sum and count
        mean = data / count
        mean[count == 0] = nodata
        mean = mean.astype('int16')

        write_img(mean, out_file,
                  nodata=nodata, geobox=geobox, options=options)

        write_img(count, count_file,
                  nodata=0, geobox=geobox, options=options)

    def unpack_group(group):
        for product_name in group:
            for band in group[product_name]:
                if not band.endswith('_pixelcount'):
                    unpack_dataset(group[product_name], product_name, band)

    with h5py.File(input_file) as fid:
        dataset = fid[list(fid)[0]]

        for group_name in dataset:
            if group_name == 'REFLECTANCE':
                # synthetic group created by merging `.h5` containers
                unpack_group(dataset[group_name])
            elif group_name.startswith('RES-GROUP'):
                # original `h5` container produced by `wagl`
                unpack_group(dataset[group_name][GroupName.STANDARD_GROUP.value]['REFLECTANCE'])


def assert_ints(array):
    if not isinstance(array, numpy.ndarray):
        array = numpy.array(array)

    assert numpy.allclose(array, numpy.around(array))
    return numpy.around(array).astype('int32')


class GeoBox:
    def __init__(self, affine, shape):
        self.affine = affine
        self.shape = shape

    def linear_part(self):
        """
        The linear transformation associated with an affine transformation.
        """
        aff = self.affine
        return numpy.array([[aff.a, aff.b], [aff.d, aff.e]])

    def shift_part(self):
        """
        The translation vector associated with an affine transformation.
        """
        aff = self.affine
        return numpy.array([aff.c, aff.f])

    def get_shape_xy(self):
        return (self.shape[1], self.shape[0])

    def corner_coords(self):
        return self.affine * self.get_shape_xy()

    def origin_coords(self):
        return self.affine * (0, 0)

    def __or__(self, other):
        """
        Given two affine descriptions of image grids, create a grid
        that encompasses both images.
        """
        assert numpy.allclose(self.linear_part(), other.linear_part())

        translation = other.shift_part() - self.shift_part()
        linear = self.linear_part()
        steps = assert_ints(numpy.linalg.solve(linear, translation))

        shift = numpy.array(self.affine * numpy.where(steps > 0, 0, steps))
        affine = Affine(linear[0, 0], linear[0, 1], shift[0], linear[1, 0], linear[1, 1], shift[1])

        self_end = ~affine * self.corner_coords()
        other_end = ~affine * other.corner_coords()
        shape = (int(max(self_end[1], other_end[1])), int(max(self_end[0], other_end[0])))

        return GeoBox(affine, shape)

    @staticmethod
    def from_dataset(dataset):
        return GeoBox(Affine.from_gdal(*dataset.attrs['geotransform']), dataset.shape)

    def window(self, other):
        ul = assert_ints(~self.affine * other.origin_coords())
        lr = assert_ints(~self.affine * other.corner_coords())

        assert numpy.all(ul >= 0)
        assert numpy.all(lr >= 0)

        shape = self.get_shape_xy()

        assert ul[0] <= shape[0]
        assert ul[1] <= shape[1]
        assert lr[0] <= shape[0]
        assert lr[1] <= shape[1]

        return (slice(ul[1], lr[1]), slice(ul[0], lr[0]))


def sum_and_count(src_product_group, src_mask, band_name):
    """
    Sum and count of pixels of surface reflectance images.
    """
    src_ds = find_dataset_by_name(src_product_group, band_name)
    src_count = find_dataset_by_name(src_product_group, band_name + "_pixelcount")

    src_data = src_ds[:]
    if src_count is None:
        count_data = numpy.where(src_data == src_ds.attrs['no_data_value'], 0, 1)
        if src_mask is not None:
            if count_data.shape != src_mask.shape:
                raise ValueError("image and mask have different shapes")
            count_data[src_mask] = 0

        src_valid_data = numpy.where(count_data > 0, src_data, 0)
        src_data = src_valid_data
    else:
        count_data = src_count[:]

    return src_data, count_data


def copy_dataset(src_product_group, src_mask, target_product_group, band_name):
    """
    If only one dataset has this band, then just copy it over to the target dataset.
    """
    src_ds = find_dataset_by_name(src_product_group, band_name)

    src_data, count_data = sum_and_count(src_product_group, src_mask, band_name)

    target_ds = target_product_group.create_dataset(band_name, data=src_data, dtype='int32', chunks=(1024, 1024))
    target_count = target_product_group.create_dataset(band_name + "_pixelcount",
                                                       data=count_data, dtype='int32', chunks=(1024, 1024))

    for key in src_ds.attrs:
        target_ds.attrs[key] = src_ds.attrs[key]
        target_count.attrs[key] = src_ds.attrs[key]

    target_count.attrs['alias'] = band_name + "_pixelcount"


def merge_datasets(left_product_group, left_mask, right_product_group, right_mask, target_product_group, band_name):
    """
    Merge images from two `.h5` groups in two different `.h5` container.
    """
    left_ds = find_dataset_by_name(left_product_group, band_name)
    right_ds = find_dataset_by_name(right_product_group, band_name)

    if left_ds.attrs['crs_wkt'] != right_ds.attrs['crs_wkt']:
        raise ValueError("I can't merge images from different CRSs yet")

    left_box = GeoBox.from_dataset(left_ds)
    right_box = GeoBox.from_dataset(right_ds)
    target_box = left_box | right_box

    target_ds = target_product_group.create_dataset(band_name, shape=target_box.shape,
                                                    dtype='int32', chunks=(1024, 1024))
    target_ds[:] = 0
    target_count = target_product_group.create_dataset(band_name + "_pixelcount", shape=target_box.shape,
                                                       dtype='int32', chunks=(1024, 1024))
    target_count[:] = 0

    def add_image(src_box, src_product, src_mask):
        """
        Add this image to the target array.
        """
        window = target_box.window(src_box)

        src_data, count_data = sum_and_count(src_product, src_mask, band_name)

        # add the data to appropriate window in the target array
        target_ds[window] += src_data
        target_count[window] += count_data

    add_image(left_box, left_product_group, left_mask)
    add_image(right_box, right_product_group, right_mask)

    # copy over common metadata
    for key in left_ds.attrs:
        if key in right_ds.attrs:
            try:
                if left_ds.attrs[key] == right_ds.attrs[key]:
                    target_ds.attrs[key] = left_ds.attrs[key]
                    target_count.attrs[key] = left_ds.attrs[key]
            except ValueError:
                # value is a numpy array
                if numpy.all(left_ds.attrs[key] == right_ds.attrs[key]):
                    target_ds.attrs[key] = left_ds.attrs[key]
                    target_count.attrs[key] = left_ds.attrs[key]

    target_ds.attrs['geotransform'] = target_box.affine.to_gdal()
    target_ds.attrs['alias'] = band_name
    target_count.attrs['geotransform'] = target_box.affine.to_gdal()
    target_count.attrs['alias'] = band_name + "_pixelcount"


def merge_groups(left_group, left_mask, right_group, right_mask, target_group):
    """
    Merge groups from the `.h5` containers.
    """
    product_names = set(left_group)
    if product_names != set(right_group):
        raise ValueError("products from two groups do not match")

    for product_name in product_names:
        target_product_group = target_group.create_group(product_name)

        band_names = {left_group[product_name][band].attrs['alias']
                      for band in left_group[product_name] if not band.endswith("_pixelcount")}
        band_names |= {right_group[product_name][band].attrs['alias']
                       for band in right_group[product_name] if not band.endswith("_pixelcount")}

        for band_name in band_names:
            if find_dataset_by_name(left_group[product_name], band_name) is None:
                copy_dataset(right_group[product_name], right_mask, target_product_group, band_name)

            elif find_dataset_by_name(right_group[product_name], band_name) is None:
                copy_dataset(left_group[product_name], left_mask, target_product_group, band_name)

            else:
                merge_datasets(left_group[product_name], left_mask,
                               right_group[product_name], right_mask,
                               target_product_group, band_name)


def filename_and_mask(luigi_target):
    filename = luigi_target['images'].path
    if 'fmask' in luigi_target:
        with rasterio.open(luigi_target['fmask'].path) as mask_file:
            # we will mask out these pixels
            mask = mask_file.read(1) != 1
    else:
        mask = None

    return filename, mask


def merge_images(left, right, target):
    """
    Merge all images in two containers.
    """
    left_filename, left_mask = filename_and_mask(left)
    right_filename, right_mask = filename_and_mask(right)

    with h5py.File(target) as target_fid, \
            h5py.File(left_filename, 'r') as left_fid, \
            h5py.File(right_filename, 'r') as right_fid:

        if len(left_fid) != 1 or len(right_fid) != 1:
            raise ValueError('multiple granules not supported')

        left_granule = left_fid[list(left_fid)[0]]
        right_granule = right_fid[list(right_fid)[0]]
        target_granule = target_fid.create_group('synthetic')
        target_group = target_granule.create_group('REFLECTANCE')

        std_group = GroupName.STANDARD_GROUP.value

        def reflectance(granule):
            if 'REFLECTANCE' in granule:
                # synthetic .h5
                return granule['REFLECTANCE']

            # wagl .h5
            for group_name in granule:
                if group_name.startswith('RES-GROUP'):
                    return granule[group_name][std_group]['REFLECTANCE']

            raise ValueError('reflectance group not found')

        merge_groups(reflectance(left_granule), left_mask,
                     reflectance(right_granule), right_mask,
                     target_group)


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
            for product_name in group:
                for band in group[product_name]:
                    ds = group[product_name][band]

                    band_name = ds.attrs['alias']
                    data = numpy.where(valid_pixels, mask_invalid(ds[:]), numpy.nan)

                    yield [product_name, date, granule, band_name,
                           tag, numpy.nanmean(data), numpy.nanstd(data), numpy.sum(valid_pixels)]

        for group_name in dataset:
            if group_name.startswith('RES-GROUP'):
                yield from process(dataset[group_name][GroupName.STANDARD_GROUP.value]['REFLECTANCE'])
