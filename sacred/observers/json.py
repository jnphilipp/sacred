# -*- coding: utf-8 -*-

import csv
import json
import os

from sacred.observers.base import RunObserver
from sacred.serializer import flatten


class JSONObserver(RunObserver):
    def __init__(self, base_dir, _id=None, number_format='%03d', indent=4,
                 ensure_ascii=False):
        self.base_dir = base_dir
        self._id = int(_id) if _id is not None else -1
        self.number_format = number_format
        self.indent = indent
        self.ensure_ascii = ensure_ascii
        self.run_dir = None
        self.run_entry = None
        self.cout = ""

    def create_dirs(self, name, _id):
        experiments_dir = os.path.join(self.base_dir, name)
        if not os.path.exists(experiments_dir):
            os.makedirs(experiments_dir)

        if _id is None and self._id is -1:
            self._id = sum(1 for e in os.scandir(experiments_dir)
                           if e.is_dir() and e.name.isdigit())
        self.run_dir = os.path.join(experiments_dir,
                                    self.number_format % self._id)
        if not os.path.exists(self.run_dir):
            os.makedirs(self.run_dir)

    def queued_event(self, ex_info, command, host_info, queue_time, config,
                     meta_info, _id):
        self.create_dirs(ex_info['name'], _id)

        self.run_entry = {
            '_id': self.number_format % self._id,
            'experiment': dict(ex_info),
            'command': command,
            'host': dict(host_info),
            'meta': meta_info,
            'status': 'QUEUED',
            'config': config
        }
        self.save()
        return self._id if _id is None else _id

    def started_event(self, ex_info, command, host_info, start_time, config,
                      meta_info, _id):
        self.create_dirs(ex_info['name'], _id)

        self.cout = ""
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
            'info': {},
        }
        self.save()
        return self._id if _id is None else _id

    def heartbeat_event(self, info, captured_out, beat_time, result):
        self.run_entry['info'] = info
        self.run_entry['result'] = result
        self.cout = captured_out
        self.save()

    def completed_event(self, stop_time, result):
        self.run_entry['stop_time'] = stop_time.isoformat()
        self.run_entry['result'] = result
        self.run_entry['status'] = 'COMPLETED'
        self.save()

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

    def artifact_event(self, name, filename, metadata=None, content_type=None):
        self.run_entry['artifacts'].append(name)
        self.save()

    def log_metrics(self, metrics_by_name, info):
        """Store new measurements into sacred-metrics-COMMAND.json.
        """
        if os.path.exists(self.run_dir):
            filename = 'sacred-metrics-%s.json' % self.run_entry['command']

            metrics = {}
            metrics_path = os.path.join(self.run_dir, filename)
            if os.path.exists(metrics_path):
                with open(metrics_path, 'r', encoding='utf-8') as f:
                    metrics = json.load(f)

        for k, v in metrics_by_name.items():
            if k not in metrics:
                metrics[k] = {'values': [], 'steps': [], 'timestamps': []}

            metrics[k]['values'] += v['values']
            metrics[k]['steps'] += v['steps']

            # Manually convert them to avoid passing a datetime dtype handler
            # when we're trying to convert into json.
            timestamps = [ts.isoformat() for ts in v['timestamps']]
            metrics[k]['timestamps'] += timestamps
        self.save_json(filename, metrics)

    def save(self):
        if os.path.exists(self.run_dir):
            filename = 'sacred-%s.json' % self.run_entry['command']
            self.save_json(filename, self.run_entry)

            filename = 'sacred-%s.out' % self.run_entry['command']
            with open(os.path.join(self.run_dir, filename), 'wb') as f:
                f.write(self.cout.encode('utf-8'))

    def save_json(self, filename, obj):
        with open(os.path.join(self.run_dir, filename), 'w',
                  encoding='utf-8') as f:
            f.write(json.dumps(flatten(obj), sort_keys=True,
                               indent=self.indent,
                               ensure_ascii=self.ensure_ascii))
            f.write('\n')

    def __eq__(self, other):
        if isinstance(other, JSONObserver):
            return self.runs == other.runs
        return False

    def __ne__(self, other):
        return not self.__eq__(other)
