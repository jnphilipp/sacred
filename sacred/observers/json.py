# -*- coding: utf-8 -*-

import csv
import json
import os

from bson import json_util
from sacred.observers.base import RunObserver


class JSONObserver(RunObserver):
    @classmethod
    def create(cls, base_dir, _id=None, number_format='%03d', indent=4):
        if not os.path.exists(base_dir):
            os.makedirs(base_dir)
        return cls(base_dir, _id, number_format, indent)

    def __init__(self, base_dir, _id, number_format, indent):
        self.base_dir = base_dir
        self._id = int(_id) if _id is not None else -1
        self.number_format = number_format
        self.indent = indent
        self.experiment_dir = None
        self.run_entry = None

    def create_dirs(self, name, _id):
        experiments_dir = os.path.join(self.base_dir, name)
        if not os.path.exists(experiments_dir):
            os.makedirs(experiments_dir)

        if _id is None and self._id is -1:
            self._id = sum(1 for e in os.scandir(experiments_dir) if e.is_dir())
        self.experiment_dir = os.path.join(experiments_dir,
                                           self.number_format % self._id)
        if not os.path.exists(self.experiment_dir):
            os.makedirs(self.experiment_dir)

    def queued_event(self, ex_info, command, host_info, queue_time, config,
                     meta_info, _id):
        self.run_entry = {
            '_id': self.number_format % self._id,
            'experiment': dict(ex_info),
            'command': command,
            'host': dict(host_info),
            'meta': meta_info,
            'status': 'QUEUED',
            'config': config
        }
        self.create_dirs(ex_info['name'], _id)
        self.save()
        return self._id if _id is None else _id

    def started_event(self, ex_info, command, host_info, start_time, config,
                      meta_info, _id):
        self.run_entry = {
            '_id': self.number_format % self._id,
            'experiment': dict(ex_info),
            'command': command,
            'host': dict(host_info),
            'start_time': start_time.isoformat(),
            'config': config,
            'meta': meta_info,
            'status': 'RUNNING',
            'resources': [],
            'artifacts': [],
            'captured_out': '',
            'info': {},
        }
        self.create_dirs(ex_info['name'], _id)
        self.save()
        return self._id if _id is None else _id

    def heartbeat_event(self, info, captured_out, beat_time, result):
        self.run_entry['info'] = info
        self.run_entry['captured_out'] = captured_out
        self.run_entry['result'] = result
        self.save()

    def completed_event(self, stop_time, result):
        self.run_entry['stop_time'] = stop_time.isoformat()
        self.run_entry['result'] = result
        self.run_entry['status'] = 'COMPLETED'
        self.save()

        if result:
            experiments_path = os.path.join(
                self.base_dir,
                self.run_entry['experiment']['name'],
                'experiments.csv'
            )
            ex = self.number_format % self._id
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
                        if row['experiment'] == ex:
                            for k in row.keys():
                                if k is not 'experiment' and k not in result:
                                    result[k] = row[k]
                        else:
                            experiments.append(row)

            with open(experiments_path, 'w', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fields, dialect='unix')
                writer.writeheader()
                writer.writerows(experiments)
                result['experiment'] = ex
                writer.writerow(result)

    def interrupted_event(self, interrupt_time, status):
        self.run_entry['stop_time'] = interrupt_time.isoformat()
        self.run_entry['status'] = status
        self.save()

    def failed_event(self, fail_time, fail_trace):
        self.run_entry['stop_time'] = fail_time.isoformat()
        self.run_entry['status'] = 'FAILED'
        self.run_entry['fail_trace'] = fail_trace
        self.save()

    def resource_event(self, filename):
        self.run_entry['resources'].append(filename)
        self.save()

    def artifact_event(self, name, filename):
        self.run_entry['artifacts'].append(name)
        self.save()

    def save(self):
        if os.path.exists(self.experiment_dir):
            filename = 'sacred-%s.json' % self.run_entry['command']
            with open(os.path.join(self.experiment_dir, filename), 'w',
                      encoding='utf8') as f:
                f.write(json.dumps(self.run_entry, indent=self.indent,
                                   default=json_util.default))
                f.write('\n')

    def __eq__(self, other):
        if isinstance(other, JSONObserver):
            return self.runs == other.runs
        return False

    def __ne__(self, other):
        return not self.__eq__(other)
