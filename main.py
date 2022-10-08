# coding=utf-8

import os
import sys
import logging
import json
import operator
import time
from datetime import datetime

log_level = logging.INFO
if os.getenv('DEBUG', False):
    log_level = logging.DEBUG

logging.basicConfig(
    format='%(asctime)s %(levelname)-7s %(message)s',
    datefmt='%Y-%d-%m %H:%M:%S',
    level=log_level
)


class HBSLoganalyzer:
    def __init__(self):
        self.jobs_by_uuid = {}
        self.jobs_by_name = {}
        self.needle_job = None
        self.job_path = None
        self.run_log_entries = {}
        self.analyze_number_of_runs = None
        self.data_path = "/share/CACHEDEV2_DATA/.qpkg/HybridBackup/data/system/nas/"  # Todo: this should be automatic
        self.recurring_files = {}
        self.file_sizes = {}
        self.size_tree = {}
        self.report_lines = 5
        self.output_dir = "/share/homes/oxi/"
        self.timestamp = time.time()
        self.time_string = datetime.fromtimestamp(self.timestamp, tz=None)

    def get_job_name(self, uuid):
        return self.jobs_by_uuid[uuid]

    def get_job_uuid(self, name):
        return self.jobs_by_name[name]

    def get_jobs(self):
        log_lines = []
        search_for = "Start job [system]"
        logfile_path = os.path.join(self.data_path, "rra.log")
        logging.info("Jobs logfile: %s" % logfile_path)
        with open(logfile_path) as f:
            for line in f.readlines():
                # logging.debug("Reading line: %s" % line)
                if search_for in line:
                    logging.debug("Found line: %s" % line)
                    log_lines.append(line.split(search_for)[1].strip())
        logging.debug("Found %s lines" % len(log_lines))

        for entry in list(set(log_lines)):
            logging.debug("Analyzing entry: %s" % entry)
            name, uuid = entry.split("][")
            name = name.replace("[", "")
            uuid = uuid.replace("]", "")
            self.jobs_by_uuid[uuid] = name
            self.jobs_by_name[name] = uuid
        logging.info("Found %s jobs" % len(self.jobs_by_name))
        for job in self.jobs_by_name:
            logging.info("\t%-25s %s" % (job, self.jobs_by_name[job]))

    def parse_job(self):
        log_lines = []
        search_for = " : history: "
        run_start_indicator = "job create history"
        current_run = None
        first_run_start_indicator_found = False

        try:
            self.needle_job = sys.argv[1]
        except IndexError:
            logging.error("Not enough arguments. Please supply job name you are interested in!")

        self.job_path = os.path.join(self.data_path, self.get_job_uuid(self.needle_job))
        job_logfile_path = os.path.join(self.job_path, "rra.log")
        logging.info("Analyzing job logfile in: %s" % self.job_path)
        with open(job_logfile_path) as f:
            for line in f.readlines():
                if run_start_indicator in line:
                    if not first_run_start_indicator_found:
                        first_run_start_indicator_found = True
                    current_run = line.split("][")[0].replace("[", "").strip()
                    self.run_log_entries[current_run] = {}
                    logging.debug("Run start indicator found for %s" % current_run)

                elif search_for in line:
                    if not first_run_start_indicator_found:
                        logging.debug("Skipping line since no run start line has been found yet")
                        continue

                    meta, json_data = line.split(search_for)
                    data = json.loads(json_data.replace("'", "\""))  # This is some REALLY dirty shit
                    if "transferring" in data.keys():
                        for file_data in data["transferring"]:
                            self.run_log_entries[current_run][file_data["path"]] = file_data["total_bytes"]

        logging.debug("Found %s runs:" % len(self.run_log_entries))
        for run in sorted(self.run_log_entries.keys()):
            tbytes = 0
            for path in self.run_log_entries[run].keys():
                tbytes += self.run_log_entries[run][path]
            logging.debug("\t%-25s %s files with %s bytes transferred" % (run, len(self.run_log_entries[run]), tbytes))

    def analyze_job(self):

        try:
            if len(self.run_log_entries.keys()) >= int(sys.argv[2]):
                self.analyze_number_of_runs = int(sys.argv[2])
                logging.info("Analyzing the last %s runs" % self.analyze_number_of_runs)
            else:
                self.analyze_number_of_runs = len(self.run_log_entries.keys())
                logging.info("Analyzing all runs")
        except IndexError:
            self.analyze_number_of_runs = len(self.run_log_entries.keys())
            logging.info("Analyzing all runs")

        self.calculate_recurring_files_data()
        self.calculate_file_sizes_data()
        self.calculate_size_tree_data()

    def calculate_recurring_files_data(self):
        logging.debug("Analyzing data for recurring files")
        loop_count = 0
        for run in sorted(self.run_log_entries.keys(), reverse=True):
            loop_count += 1
            if loop_count > self.analyze_number_of_runs:
                break
            logging.debug("\tAnalyzing data for job: %s" % run)
            for path in self.run_log_entries[run]:
                if path not in self.recurring_files.keys():
                    # logging.debug("Located new path: %s" % path)
                    self.recurring_files[path] = 0
                self.recurring_files[path] += 1

    def calculate_file_sizes_data(self):
        logging.debug("Analyzing data for file sizes")
        loop_count = 0
        for run in sorted(self.run_log_entries.keys(), reverse=True):
            loop_count += 1
            if loop_count > self.analyze_number_of_runs:
                break
            logging.debug("\tAnalyzing data for job: %s" % run)
            for path in self.run_log_entries[run]:
                if path not in self.file_sizes.keys():
                    # logging.debug("Located new path: %s" % path)
                    self.file_sizes[path] = 0
                self.file_sizes[path] += self.run_log_entries[run][path]

    def calculate_size_tree_data(self):
        def path_split(rpath):
            rhead, rtail = os.path.split(rpath)
            if rtail:
                return rhead, rtail
            else:
                return path_split(rpath.rstrip("/"))

        logging.debug("Analyzing data for size tree")
        loop_count = 0
        for run in sorted(self.run_log_entries.keys(), reverse=True):
            loop_count += 1
            if loop_count > self.analyze_number_of_runs:
                break
            logging.debug("\tAnalyzing data for job: %s" % run)
            for path in self.run_log_entries[run]:
                current_path = path
                while True:
                    head, tail = path_split(current_path)
                    if not head:
                        break
                    if head not in self.size_tree.keys():
                        self.size_tree[head] = 0
                    self.size_tree[head] += self.run_log_entries[run][path]
                    current_path = head

    def create_report(self):
        try:
            self.report_lines = int(sys.argv[3])
        except IndexError:
            logging.info("Maximum number of lines to output for the report is set to %s" % self.report_lines)

        logging.info("Listing top recurring files")
        sorted_recurring_files = sorted(self.recurring_files.items(), key=operator.itemgetter(1), reverse=True)
        output_loop_count = 0
        file_output = ["Occurrence;Path"]
        for path, occurrence in sorted_recurring_files:
            output_loop_count += 1
            if output_loop_count <= self.report_lines:
                logging.info("%-15s %s" % (occurrence, path))
            file_output.append("%s;%s" % (occurrence, path.encode("UTF-8")))
        output_file_path = os.path.join(self.output_dir, "hbs3_" + str(int(self.timestamp)) + "_file_occurrence.csv")
        logging.info("Saving full results to: %s" % output_file_path)
        with open(output_file_path, 'w') as fp:
            fp.write('\n'.join(file_output) + '\n')

        logging.info("Listing top file concerning transmission size")
        sorted_file_sizes = sorted(self.file_sizes.items(), key=operator.itemgetter(1), reverse=True)
        output_loop_count = 0
        file_output = ["Size in bytes;File"]
        for path, size in sorted_file_sizes:
            output_loop_count += 1
            if output_loop_count <= self.report_lines:
                logging.info("%-15s %s" % (size, path))
            file_output.append("%s;%s" % (size, path.encode("UTF-8")))
        output_file_path = os.path.join(self.output_dir, "hbs3_" + str(int(self.timestamp)) + "_file_size.csv")
        logging.info("Saving full results to: %s" % output_file_path)
        with open(output_file_path, 'w') as fp:
            fp.write('\n'.join(file_output) + '\n')

        logging.info("Listing top tree concerning transmission sizes")
        sorted_size_tree = sorted(self.size_tree.items(), key=operator.itemgetter(1), reverse=True)
        output_loop_count = 0
        file_output = ["Size in bytes;Path"]
        for path, size in sorted_size_tree:
            output_loop_count += 1
            if output_loop_count <= self.report_lines:
                logging.info("%-15s %s" % (size, path))
            file_output.append("%s;%s" % (size, path.encode("UTF-8")))
        output_file_path = os.path.join(self.output_dir, "hbs3_" + str(int(self.timestamp)) + "_tree_size.csv")
        logging.info("Saving full results to: %s" % output_file_path)
        with open(output_file_path, 'w') as fp:
            fp.write('\n'.join(file_output) + '\n')

    def run(self):
        logging.info("QNAP HBS 3 Loganalyzer run %s (%s)" % (self.time_string, self.timestamp))
        self.get_jobs()
        self.parse_job()
        self.analyze_job()
        self.create_report()


if __name__ == '__main__':
    la = HBSLoganalyzer()
    la.run()
