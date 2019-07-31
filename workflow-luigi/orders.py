#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Version: 0.1a1

import luigi
from luigi.contrib.external_program import ExternalProgramTask


class DownloadFromS3(ExternalProgramTask):
    path = luigi.Parameter()
    src = 's3://korniichuk.demo/workflow/input'
    dst = 'data'

    def program_args(self):
        self.output().makedirs()
        return ['aws', 's3', 'sync', self.src, self.dst]

    def output(self):
        return luigi.LocalTarget(self.dst)


class Decompress(ExternalProgramTask):
    def requires(self):
        return luigi.task.flatten(DownloadFromS3())

    def run(self):
        pass


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
