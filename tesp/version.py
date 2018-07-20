#!/usr/bin/env python
"""
Get the current version number.

Public releases are expected to be tagged in the repository
with prefix 'tesp-' and a version number following PEP-440.

Eg. ::

    tesp-0.1.0
    tesp-0.2.0.dev1
    tesp-0.2.0+9f43bbc

Refer to PEP440: https://www.python.org/dev/peps/pep-0440

This script is derived from https://github.com/Changaco/version.py
"""

from __future__ import absolute_import, print_function

import re
from os.path import dirname, isdir, join, exists
from subprocess import CalledProcessError, check_output

import pkg_resources

PREFIX = 'tesp-'

GIT_TAG_PATTERN = re.compile(r'\btag: %s([0-9][^,]*)\b' % PREFIX)
VERSION_PATTERN = re.compile(r'^Version: (.+)$', re.M)
NUMERIC = re.compile(r'^\d+$')

GIT_ARCHIVE_REF_NAMES = '$Format:%D$'
GIT_ARCHIVE_COMMIT_HASH = '$Format:%h$'


def get_version():
    package_dir = dirname(dirname(__file__))
    git_dir = join(package_dir, '.git')
    pkg_info_file = join(package_dir, 'PKG-INFO')

    if isdir(git_dir):
        # Ask git for an annotated version number
        # (eg. "tesp-0.0.0-651-gcf335a9-dirty")
        cmd = [
            'git',
            '--git-dir', git_dir,
            'describe', '--tags', '--match', PREFIX + '[0-9]*', '--dirty'
        ]
        try:
            git_version = check_output(cmd).decode().strip()[len(PREFIX):]
        except CalledProcessError:
            raise RuntimeError('Unable to get version number from git tags')
        components = git_version.split('-')
        version = components.pop(0)

        # Any other suffixes imply this is not a release: Append an internal build number
        if components:
            # <commit count>.<git hash>.<whether the working tree is dirty>
            version += '+' + '.'.join(components)

    elif exists(pkg_info_file):
        # Extract the version from the PKG-INFO file.
        with open(pkg_info_file) as f:
            version = VERSION_PATTERN.search(f.read()).group(1)
    elif not GIT_ARCHIVE_REF_NAMES.startswith('$'):
        # Return the version if it has been injected into the file by git-archive
        version_match = GIT_TAG_PATTERN.search(GIT_ARCHIVE_REF_NAMES)
        if version_match:
            version = version_match.group(1)
        else:
            # Otherwise this is not a release: just use the commit hash.
            # We can't get the last tagged version from git-archive, so we'll use 0.0.0.
            version = '0.0.0+' + GIT_ARCHIVE_COMMIT_HASH
    else:
        try:
            version = pkg_resources.get_distribution('tesp').version
        except pkg_resources.DistributionNotFound:
            raise RuntimeError('Unknown version: Package is not installed, not a '
                               'git repository, a python dist tarball'
                               'or a git-created archive.')

    return version


REPO_URL = 'https://github.com/OpenDataCubePipelines/opendatacubepipeline.tesp'


if __name__ == '__main__':
    print(get_version())
