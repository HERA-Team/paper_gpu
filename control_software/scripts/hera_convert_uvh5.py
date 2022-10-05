#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import argparse
from paper_gpu.file_conversion import make_uvh5_file


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i", "--input_file", required=True, help="name of input data file"
    )
    parser.add_argument(
        "-m", "--meta_file", required=True, help="name of input metadata file"
    )
    parser.add_argument(
        "-o", "--output_file", required=True, help="name of output UVH5 file"
    )

    args = parser.parse_args()

    if not os.path.exists(args.output_file):
        make_uvh5_file(args.output_file, args.meta_file, args.input_file)
