# -*- coding: utf-8 -*-

import csv
import json
import os

from bson import json_util
from sacred.dependencies import get_digest
from sacred.observers.base import RunObserver


class JSONObserver(RunObserver):
    def __init__(self, base_dir, number_format='%03d', indent=4):
        self.base_dir = base_dir
        self.number_format = number_format
        self.indent = indent
        self.experiment_dir = None
        self.nb_experiment = -1
        self.run_entry = None


    def started_event(self, ex_info, host_info, start_time, config, comment):
        self.run_entry = {
            'experiment': dict(ex_info),
            'host': dict(host_info),
            'start_time': start_time,
            'config': config,
            'comment': comment,
            'status': 'RUNNING',
            'resources': [],
            'artifacts': [],
            'captured_out': '',
            'info': {},
        }

        experiments_dir = os.path.join(self.base_dir, self.run_entry['experiment']['name'])
        if not os.path.exists(experiments_dir):
            os.makedirs(experiments_dir)

        self.nb_experiment = sum(1 for e in os.scandir(experiments_dir) if e.is_dir())
        self.experiment_dir = os.path.join(self.base_dir, self.run_entry['experiment']['name'], self.number_format % self.nb_experiment)
        os.makedirs(self.experiment_dir)
        self.save()


    def heartbeat_event(self, info, captured_out, beat_time):
        self.run_entry['info'] = info
        self.run_entry['captured_out'] = captured_out
        self.save()


    def completed_event(self, stop_time, result):
        self.run_entry['stop_time'] = stop_time
        self.run_entry['result'] = result
        self.run_entry['status'] = 'COMPLETED'
        self.save()

        if result:
            experiments_path = os.path.join(self.base_dir, self.run_entry['experiment']['name'], 'experiments.csv')
            fields = ['experiment'] + list(result.keys())
            experiments = []
            if os.path.exists(experiments_path):
                with open(experiments_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    fields = reader.fieldnames
                    for k in result.keys():
                        if k not in fields:
                            fields.append(k)
                    for row in reader:
                        experiments.append(row)

            with open(experiments_path, 'w', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fields, dialect='unix')
                writer.writeheader()
                writer.writerows(experiments)
                result.update(experiment=self.number_format % self.nb_experiment)
                writer.writerow(result)


    def interrupted_event(self, interrupt_time):
        self.run_entry['stop_time'] = interrupt_time
        self.run_entry['status'] = 'INTERRUPTED'
        self.save()


    def failed_event(self, fail_time, fail_trace):
        self.run_entry['stop_time'] = fail_time
        self.run_entry['status'] = 'FAILED'
        self.run_entry['fail_trace'] = fail_trace
        self.save()


    def resource_event(self, filename):
        md5hash = get_digest(filename)
        self.run_entry['resources'].append((filename, md5hash))
        self.save()


    def artifact_event(self, filename):
        self.run_entry['artifacts'].append(filename)
        self.save()


    def save(self):
        with open(os.path.join(self.experiment_dir, 'sacred.json'), 'w', encoding='utf8') as f:
            f.write(json.dumps(self.run_entry, indent=4, default=json_util.default))
            f.write('\n')


    def __eq__(self, other):
        if isinstance(other, JSONObserver):
            return self.runs == other.runs
        return False


    def __ne__(self, other):
        return not self.__eq__(other)
