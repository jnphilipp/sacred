#!/usr/bin/env python
# coding=utf-8
from __future__ import division, print_function, unicode_literals
import datetime
import mock
import pytest

from sacred.metrics_logger import ScalarMetricLogEntry, linearize_metrics

pymongo = pytest.importorskip("pymongo")
mongomock = pytest.importorskip("mongomock")

from sacred.dependencies import get_digest
from sacred.observers.mongo import (MongoObserver, force_bson_encodeable)

T1 = datetime.datetime(1999, 5, 4, 3, 2, 1)
T2 = datetime.datetime(1999, 5, 5, 5, 5, 5)


@pytest.fixture
def mongo_obs():
    db = mongomock.MongoClient().db
    runs = db.runs
    metrics = db.metrics
    fs = mock.MagicMock()
    return MongoObserver(runs, fs, metrics_collection=metrics)


@pytest.fixture()
def sample_run():
    exp = {'name': 'test_exp', 'sources': [], 'doc': '', 'base_dir': '/tmp'}
    host = {'hostname': 'test_host', 'cpu_count': 1, 'python_version': '3.4'}
    config = {'config': 'True', 'foo': 'bar', 'answer': 42}
    command = 'run'
    meta_info = {'comment': 'test run'}
    return {
        '_id': 'FEDCBA9876543210',
        'ex_info': exp,
        'command': command,
        'host_info': host,
        'start_time': T1,
        'config': config,
        'meta_info': meta_info,
    }


def test_mongo_observer_started_event_creates_run(mongo_obs, sample_run):
    sample_run['_id'] = None
    _id = mongo_obs.started_event(**sample_run)
    assert _id is not None
    assert mongo_obs.runs.count() == 1
    db_run = mongo_obs.runs.find_one()
    assert db_run == {
        '_id': _id,
        'experiment': sample_run['ex_info'],
        'format': mongo_obs.VERSION,
        'command': sample_run['command'],
        'host': sample_run['host_info'],
        'start_time': sample_run['start_time'],
        'heartbeat': None,
        'info': {},
        'captured_out': '',
        'artifacts': [],
        'config': sample_run['config'],
        'meta': sample_run['meta_info'],
        'status': 'RUNNING',
        'resources': []
    }


def test_mongo_observer_started_event_uses_given_id(mongo_obs, sample_run):
    _id = mongo_obs.started_event(**sample_run)
    assert _id == sample_run['_id']
    assert mongo_obs.runs.count() == 1
    db_run = mongo_obs.runs.find_one()
    assert db_run['_id'] == sample_run['_id']


def test_mongo_observer_equality(mongo_obs):
    runs = mongo_obs.runs
    fs = mock.MagicMock()
    m = MongoObserver(runs, fs)
    assert mongo_obs == m
    assert not mongo_obs != m

    assert not mongo_obs == 'foo'
    assert mongo_obs != 'foo'


def test_mongo_observer_heartbeat_event_updates_run(mongo_obs, sample_run):
    mongo_obs.started_event(**sample_run)

    info = {'my_info': [1, 2, 3], 'nr': 7}
    outp = 'some output'
    mongo_obs.heartbeat_event(info=info, captured_out=outp, beat_time=T2,
                              result=1337)

    assert mongo_obs.runs.count() == 1
    db_run = mongo_obs.runs.find_one()
    assert db_run['heartbeat'] == T2
    assert db_run['result'] == 1337
    assert db_run['info'] == info
    assert db_run['captured_out'] == outp


def test_mongo_observer_completed_event_updates_run(mongo_obs, sample_run):
    mongo_obs.started_event(**sample_run)

    mongo_obs.completed_event(stop_time=T2, result=42)

    assert mongo_obs.runs.count() == 1
    db_run = mongo_obs.runs.find_one()
    assert db_run['stop_time'] == T2
    assert db_run['result'] == 42
    assert db_run['status'] == 'COMPLETED'


def test_mongo_observer_interrupted_event_updates_run(mongo_obs, sample_run):
    mongo_obs.started_event(**sample_run)

    mongo_obs.interrupted_event(interrupt_time=T2, status='INTERRUPTED')

    assert mongo_obs.runs.count() == 1
    db_run = mongo_obs.runs.find_one()
    assert db_run['stop_time'] == T2
    assert db_run['status'] == 'INTERRUPTED'


def test_mongo_observer_failed_event_updates_run(mongo_obs, sample_run):
    mongo_obs.started_event(**sample_run)

    fail_trace = "lots of errors and\nso\non..."
    mongo_obs.failed_event(fail_time=T2,
                           fail_trace=fail_trace)

    assert mongo_obs.runs.count() == 1
    db_run = mongo_obs.runs.find_one()
    assert db_run['stop_time'] == T2
    assert db_run['status'] == 'FAILED'
    assert db_run['fail_trace'] == fail_trace


def test_mongo_observer_artifact_event(mongo_obs, sample_run):
    mongo_obs.started_event(**sample_run)

    filename = "setup.py"
    name = 'mysetup'

    mongo_obs.artifact_event(name, filename)

    assert mongo_obs.fs.put.called
    assert mongo_obs.fs.put.call_args[1]['filename'].endswith(name)

    db_run = mongo_obs.runs.find_one()
    assert db_run['artifacts']


def test_mongo_observer_resource_event(mongo_obs, sample_run):
    mongo_obs.started_event(**sample_run)

    filename = "setup.py"
    md5 = get_digest(filename)

    mongo_obs.resource_event(filename)

    assert mongo_obs.fs.exists.called
    mongo_obs.fs.exists.assert_any_call(filename=filename)

    db_run = mongo_obs.runs.find_one()
    # for some reason py27 returns this as tuples and py36 as lists
    assert [tuple(r) for r in db_run['resources']] == [(filename, md5)]


def test_force_bson_encodable_doesnt_change_valid_document():
    d = {'int': 1, 'string': 'foo', 'float': 23.87, 'list': ['a', 1, True],
         'bool': True, 'cr4zy: _but_ [legal) Key!': '$illegal.key.as.value',
         'datetime': datetime.datetime.utcnow(), 'tuple': (1, 2.0, 'three'),
         'none': None}
    assert force_bson_encodeable(d) == d


def test_force_bson_encodable_substitutes_illegal_value_with_strings():
    d = {
        'a_module': datetime,
        'some_legal_stuff': {'foo': 'bar', 'baz': [1, 23, 4]},
        'nested': {
            'dict': {
                'with': {
                    'illegal_module': mock
                }
            }
        },
        '$illegal': 'because it starts with a $',
        'il.legal': 'because it contains a .',
        12.7: 'illegal because it is not a string key'
    }
    expected = {
        'a_module': str(datetime),
        'some_legal_stuff': {'foo': 'bar', 'baz': [1, 23, 4]},
        'nested': {
            'dict': {
                'with': {
                    'illegal_module': str(mock)
                }
            }
        },
        '@illegal': 'because it starts with a $',
        'il,legal': 'because it contains a .',
        '12,7': 'illegal because it is not a string key'
    }
    assert force_bson_encodeable(d) == expected


@pytest.fixture
def logged_metrics():
    return [
        ScalarMetricLogEntry("training.loss", 10, datetime.datetime.utcnow(), 1),
        ScalarMetricLogEntry("training.loss", 20, datetime.datetime.utcnow(), 2),
        ScalarMetricLogEntry("training.loss", 30, datetime.datetime.utcnow(), 3),

        ScalarMetricLogEntry("training.accuracy", 10, datetime.datetime.utcnow(), 100),
        ScalarMetricLogEntry("training.accuracy", 20, datetime.datetime.utcnow(), 200),
        ScalarMetricLogEntry("training.accuracy", 30, datetime.datetime.utcnow(), 300),

        ScalarMetricLogEntry("training.loss", 40, datetime.datetime.utcnow(), 10),
        ScalarMetricLogEntry("training.loss", 50, datetime.datetime.utcnow(), 20),
        ScalarMetricLogEntry("training.loss", 60, datetime.datetime.utcnow(), 30)
    ]


def test_log_metrics(mongo_obs, sample_run, logged_metrics):
    """
    Test storing scalar measurements
    
    Test whether measurements logged using _run.metrics.log_scalar_metric
    are being stored in the 'metrics' collection
    and that the experiment 'info' dictionary contains a valid reference 
    to the metrics collection for each of the metric.
    
    Metrics are identified by name (e.g.: 'training.loss') and by the 
    experiment run that produced them. Each metric contains a list of x values
    (e.g. iteration step), y values (measured values) and timestamps of when 
    each of the measurements was taken.
    """

    # Start the experiment
    mongo_obs.started_event(**sample_run)

    # Initialize the info dictionary and standard output with arbitrary values
    info = {'my_info': [1, 2, 3], 'nr': 7}
    outp = 'some output'

    # Take first 6 measured events, group them by metric name
    # and store the measured series to the 'metrics' collection
    # and reference the newly created records in the 'info' dictionary.
    mongo_obs.log_metrics(linearize_metrics(logged_metrics[:6]), info)
    # Call standard heartbeat event (store the info dictionary to the database)
    mongo_obs.heartbeat_event(info=info, captured_out=outp, beat_time=T1,
                              result=0)

    # There should be only one run stored
    assert mongo_obs.runs.count() == 1
    db_run = mongo_obs.runs.find_one()
    # ... and the info dictionary should contain a list of created metrics
    assert "metrics" in db_run['info']
    assert type(db_run['info']["metrics"]) == list

    # The metrics, stored in the metrics collection,
    # should be two (training.loss and training.accuracy)
    assert mongo_obs.metrics.count() == 2
    # Read the training.loss metric and make sure it references the correct run
    # and that the run (in the info dictionary) references the correct metric record.
    loss = mongo_obs.metrics.find_one({"name": "training.loss", "run_id": db_run['_id']})
    assert {"name": "training.loss", "id": str(loss["_id"])} in db_run['info']["metrics"]
    assert loss["steps"] == [10, 20, 30]
    assert loss["values"] == [1, 2, 3]
    for i in range(len(loss["timestamps"]) - 1):
        assert loss["timestamps"][i] <= loss["timestamps"][i + 1]

    # Read the training.accuracy metric and check the references as with the training.loss above
    accuracy = mongo_obs.metrics.find_one({"name": "training.accuracy", "run_id": db_run['_id']})
    assert {"name": "training.accuracy", "id": str(accuracy["_id"])} in db_run['info']["metrics"]
    assert accuracy["steps"] == [10, 20, 30]
    assert accuracy["values"] == [100, 200, 300]

    # Now, process the remaining events
    # The metrics shouldn't be overwritten, but appended instead.
    mongo_obs.log_metrics(linearize_metrics(logged_metrics[6:]), info)
    mongo_obs.heartbeat_event(info=info, captured_out=outp, beat_time=T2,
                              result=0)

    assert mongo_obs.runs.count() == 1
    db_run = mongo_obs.runs.find_one()
    assert "metrics" in db_run['info']

    # The newly added metrics belong to the same run and have the same names, so the total number
    # of metrics should not change.
    assert mongo_obs.metrics.count() == 2
    loss = mongo_obs.metrics.find_one({"name": "training.loss", "run_id": db_run['_id']})
    assert {"name": "training.loss", "id": str(loss["_id"])} in db_run['info']["metrics"]
    # ... but the values should be appended to the original list
    assert loss["steps"] == [10, 20, 30, 40, 50, 60]
    assert loss["values"] == [1, 2, 3, 10, 20, 30]
    for i in range(len(loss["timestamps"]) - 1):
        assert loss["timestamps"][i] <= loss["timestamps"][i + 1]

    accuracy = mongo_obs.metrics.find_one({"name": "training.accuracy", "run_id": db_run['_id']})
    assert {"name": "training.accuracy", "id": str(accuracy["_id"])} in db_run['info']["metrics"]
    assert accuracy["steps"] == [10, 20, 30]
    assert accuracy["values"] == [100, 200, 300]

    # Make sure that when starting a new experiment, new records in metrics are created
    # instead of appending to the old ones.
    sample_run["_id"] = "NEWID"
    # Start the experiment
    mongo_obs.started_event(**sample_run)
    mongo_obs.log_metrics(linearize_metrics(logged_metrics[:4]), info)
    mongo_obs.heartbeat_event(info=info, captured_out=outp, beat_time=T1,
                              result=0)
    # A new run has been created
    assert mongo_obs.runs.count() == 2
    # Another 2 metrics have been created
    assert mongo_obs.metrics.count() == 4