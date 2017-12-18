#!/usr/bin/env python

from pathlib import Path
import argparse
from eodatasets.verify import PackageChecksum


def main(out_fname):
    """
    Checksum all files adjacent to and heirarchially below the
    output file.

    :param out_fname:
        The full file pathname of the file to contain the checksums.

    :return:
        None; the output is written directly to disk given by
        `out_fname`.
    """

    out_fname = Path(out_fname)
    files = [f for f in out_fname.parent.glob('**/*') if f.is_file()]
    chksum = PackageChecksum()
    chksum.add_files(files)
    chksum.write(out_fname)


if __name__ == '__main__':
    description = "Generate a package checksum."
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument("--out_fname", required=True,
                        help=("The full file pathname of the file to contain "
                              "the checksums."))

    args = parser.parse_args()

    main(args.out_fname)
