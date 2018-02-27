# -*- coding: utf-8 -*-

import os
import re
import json
import time
from yandextank.core import AbstractPlugin
from yandextank.plugins.Aggregator import \
    AggregatorPlugin, AggregateResultListener, AbstractReader


class VegetaPlugin(AbstractPlugin, AggregateResultListener):
    SECTION = "vegeta"

    @staticmethod
    def get_key():
        return __file__

    def __init__(self, core):
        AbstractPlugin.__init__(self, core)
        self._output_file_path = None
        self.rc = -1

    def get_available_options(self):
        return ["output_file_path"]

    def configure(self):
        self._output_file_path = self.get_option("output_file_path", "")
        if not self._output_file_path:
            self.log.warning("The output_file_path option isn't given.")

    def prepare_test(self):
        aggregator = None
        try:
            aggregator = self.core.get_plugin_of_type(AggregatorPlugin)
        except Exception, ex:
            self.log.warning("No aggregator found: %s", ex)
        if aggregator:
            aggregator.reader = VegetaReader(aggregator, self)
            aggregator.add_result_listener(self)

    def start_test(self):
        pass

    def end_test(self, retcode):
        return retcode

    def post_process(self, retcode):
        return retcode

    def is_test_finished(self):
        return self.rc

    def aggregate_second(self, second_aggregate_data):
        self.log.info("VegetaPlugin: second_aggregate_data: %s"
                      % second_aggregate_data)


class VegetaReader(AbstractReader):
    """ Adapter to read vegeta out files """

    def __init__(self, owner, vegeta):
        AbstractReader.__init__(self, owner)
        self._vegeta_file = vegeta._output_file_path
        self._vegetaout = None
        self._vegeta = vegeta

    def check_open_files(self):
        if not self._vegetaout and os.path.exists(self._vegeta_file):
            self.log.info("Opening vegeta output file: %s", self._vegeta_file)
            self._vegetaout = open(self._vegeta_file, "r")

    def close_files(self):
        if self._vegetaout:
            self.log.info("Closing vegeta output file: %s", self._vegeta_file)
            self._vegetaout.close()

    def get_next_sample(self, force):
        def str2tstmp(line):
            pat = re.compile("(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)(.\d+)")
            m = pat.search(line)
            if not m:
                return 0
            (yy, mm, dd, h, m, s, ms) = m.groups()
            ts = time.mktime((int(yy), int(mm), int(dd),
                              int(h), int(m), int(s),
                              0, 0, 0))
            return ts + float(ms)

        if self._vegetaout and len(self.data_queue) < 5:
            self.log.info("Reading vegetaout, up to 10MB...")
            vegetaout = self._vegetaout.readlines(10 * 1024 * 1024)
        else:
            self.log.info("Skipped vegetaout reading")
            vegetaout = []

        if not self._vegetaout:
            self._vegeta.rc = 1
            self.log.error("Output file is undefined.")
            return None
        for line in vegetaout:
            record = json.loads(line)
            latency = int(record["latency"])
            cur_time = int(str2tstmp(record["timestamp"]) +
                           float(latency) / 1000000000)
            if cur_time not in self.data_queue:
                if self.data_queue and self.data_queue[-1] >= cur_time:
                    cur_time = self.data_queue[-1]
                else:
                    self.data_queue.append(cur_time)
                    self.data_buffer[cur_time] = []
            data_item = [cur_time, 0, latency / 1000000,
                         int(record["code"]), 0, int(record["bytes_out"]),
                         int(record["bytes_in"]), 0, 0, 0, 0, 0]
            self.data_buffer[cur_time].append(data_item)

        if len(self.data_queue):
            self._vegeta.rc = -1
            item = self.pop_second()
            return item
        else:
            self._vegeta.rc = 0
            self.log.info("No queue data!")
            return None
