#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Version: 0.1a2

import os
from subprocess import check_call

import luigi
from luigi.contrib.external_program import ExternalProgramTask


class DownloadFromS3(ExternalProgramTask):
    src = 's3://korniichuk.demo/workflow/input'
    dst = 'gz'

    def program_args(self):
        return ['aws', 's3', 'sync', self.src, self.dst]

    def output(self):
        return luigi.LocalTarget(self.dst)


class Decompress(luigi.Task):
    dst = os.path.abspath('json')

    def requires(self):
        return DownloadFromS3()

    def run(self):
        os.makedirs(self.dst)
        cwd = os.getcwd()
        dir_abs_path = os.path.abspath(self.input().path)
        os.chdir(dir_abs_path)
        for root, dirs, files in os.walk(dir_abs_path, topdown=False):
            for name in files:
                if name.endswith('.gz'):
                    file_abs_path = os.path.join(self.dst,
                                                 name.replace('.gz', ''))
                    command = 'zcat {} > {}'.format(name, file_abs_path)
                    try:
                        check_call(command, shell=True)
                    except BaseException:
                        msg = 'Error: {} file decompression is failed'.format(
                                name)
                        print(msg)
                    else:
                        msg = '{} file decompressed'.format(name)
                        print(msg)
        # Reset cwd
        os.chdir(cwd)

    def output(self):
        return luigi.LocalTarget(self.dst)


def uncompress(dir_abs_path):
    cwd = os.getcwd()

    os.chdir(dir_abs_path)
    for root, dirs, files in os.walk(dir_abs_path, topdown=False):
        for name in files:
            if name.endswith('.gz'):
                command = ['gunzip', '--keep', '--force', name]
                try:
                    check_call(command, shell=False)
                except BaseException:
                    msg = 'Error: {} file uncompression is failed.'.format(
                            name)
                    print(msg)
                else:
                    print('{} file uncompressed to {}'.format(
                            name, name.replace('.gz', '')))
    # Reset cwd
    os.chdir(cwd)


class PreprocessJSONs(luigi.Task):
    pass


class MergeCSVs(luigi.Task):
    pass


class CalcOrders(luigi.Task):
    pass


class UploadToS3(luigi.Task):
    pass


class Clean(luigi.Task):
    pass


if __name__ == '__main__':
    luigi.run()
