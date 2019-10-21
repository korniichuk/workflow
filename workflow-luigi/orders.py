#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Version: 0.1a9
# Owner: Ruslan Korniichuk

import glob
import os
from subprocess import check_call

import arrow
import boto3
import luigi
from luigi.contrib.external_program import ExternalProgramTask
from luigi.contrib.s3 import S3Target
import pandas as pd


class DownloadFromS3(ExternalProgramTask):
    src = 's3://korniichuk.demo/workflow/input'
    dst = '/tmp/luigi-orders/gz'

    def program_args(self):
        return ['aws', 's3', 'sync', self.src, self.dst]

    def output(self):
        return luigi.LocalTarget(self.dst)


class Decompress(luigi.Task):
    dst = '/tmp/luigi-orders/json'

    def requires(self):
        return DownloadFromS3()

    def run(self):
        os.makedirs(self.dst)
        cwd = os.getcwd()
        dir_abs_path = self.input().path
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


class PreprocessJSONs(luigi.Task):
    dst = '/tmp/luigi-orders/csv'

    def requires(self):
        return Decompress()

    def run(self):
        os.makedirs(self.dst)
        cwd = os.getcwd()
        dir_abs_path = self.input().path
        os.chdir(dir_abs_path)
        for root, dirs, files in os.walk(dir_abs_path, topdown=False):
            for name in files:
                ext = os.path.splitext(name)[1]
                if ext == '':
                    df = pd.read_json(name, lines=True, convert_dates=['date'])
                    df = df[['date', 'gross', 'net', 'tax', 'email']]
                    filename = name + '.csv'
                    file_abs_path = os.path.join(self.dst, filename)
                    df.to_csv(file_abs_path, index=False)
                    msg = 'Preprocessed data saved to {} file'
                    print(msg)
        # Reset cwd
        os.chdir(cwd)

    def output(self):
        return luigi.LocalTarget(self.dst)


class MergeCSVs(luigi.Task):
    date = arrow.utcnow().format('YYYYMMDD')
    dst_filename = 'transactions_{}.csv'.format(date)

    def requires(self):
        return PreprocessJSONs()

    def run(self):
        cwd = os.getcwd()
        dir_abs_path = self.input().path
        os.chdir(dir_abs_path)
        csvs = [i for i in glob.glob('*.{}'.format('csv'))]
        df = pd.concat(
                [pd.read_csv(csv, parse_dates=['date']) for csv in csvs])
        dst = os.path.join(self.input().path, self.dst_filename)
        df.to_csv(dst, index=False)
        # Reset cwd
        os.chdir(cwd)

    def output(self):
        dst = os.path.join(self.input().path, self.dst_filename)
        return luigi.LocalTarget(dst)


class UploadTransactionsToS3(luigi.Task):
    bucket_name = 'korniichuk.demo'
    dst_dirname = 'workflow/output'

    def requires(self):
        return MergeCSVs()

    def run(self):
        s3 = boto3.resource('s3')
        src = self.input().path
        dst_filename = os.path.basename(self.input().path)
        dst = os.path.join(self.dst_dirname, dst_filename)
        s3.Bucket(self.bucket_name).upload_file(src, dst)

    def output(self):
        dst_filename = os.path.basename(self.input().path)
        dst = os.path.join('s3://korniichuk.demo/workflow/output',
                           dst_filename)
        return S3Target(dst)


class CalcOrders(luigi.Task):
    date = arrow.utcnow().format('YYYYMMDD')
    dst_filename = 'orders_{}.csv'.format(date)

    def requires(self):
        return MergeCSVs()

    def run(self):
        df = pd.read_csv(self.input().path, parse_dates=['date'])
        df = df.groupby('email').count().sort_values('email')
        result = pd.DataFrame()
        result['email'] = df.index
        result['orders'] = df.date.values
        dts_dirname = os.path.dirname(self.input().path)
        dst = os.path.join(dts_dirname, self.dst_filename)
        result.to_csv(dst, index=False)

    def output(self):
        dts_dirname = os.path.dirname(self.input().path)
        dst = os.path.join(dts_dirname, self.dst_filename)
        return luigi.LocalTarget(dst)


class UploadOrdersToS3(luigi.Task):
    bucket_name = 'korniichuk.demo'
    dst_dirname = 'workflow/output'

    def requires(self):
        return CalcOrders()

    def run(self):
        s3 = boto3.resource('s3')
        src = self.input().path
        dst_filename = os.path.basename(self.input().path)
        dst = os.path.join(self.dst_dirname, dst_filename)
        s3.Bucket(self.bucket_name).upload_file(src, dst)

    def output(self):
        dst_filename = os.path.basename(self.input().path)
        dst = os.path.join('s3://korniichuk.demo/workflow/output',
                           dst_filename)
        return S3Target(dst)


class Orders(ExternalProgramTask):
    def requires(self):
        return [UploadTransactionsToS3(), UploadOrdersToS3()]

    def program_args(self):
        path = '/tmp/luigi-orders'
        return ['rm', '-rf', path]


if __name__ == '__main__':
    luigi.run()
