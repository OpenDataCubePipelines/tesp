#!/usr/bin/env python

from pathlib import Path
import argparse
from eodatasets.verify import PackageChecksum


def main(filename):
    """
    Checksum the package contents.
    """
    with open(filename, 'r') as src:
        fnames = [Path(f.strip()) for f in src.readlines()]

    chksum = PackageChecksum()
    chksum.add_files(fnames)
    chksum.write('CHECKSUM.SHA1')


if __name__ == '__main__':
    description = "Generate a package checksum."
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument("--filename", required=True,
                        help=("The filename of the file containing "
                              "the list of files to be checksummed."))

    args = parser.parse_args()

    main(args.filename)
