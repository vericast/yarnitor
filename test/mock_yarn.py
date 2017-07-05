"""Uses hypothesis to generate mock data for YARN, Spark, and MapReduce
REST resources and serves them using Flask for testing purposes.
"""
import os
import string
import time

from flask import Flask, request, json, make_response
from flask_redis import FlaskRedis
import hypothesis.strategies as st

app = Flask(__name__)
app.config["REDIS_URL"] = "redis://" + os.getenv("REDIS_ENDPOINT", "localhost:6379")
redis = FlaskRedis(app)


def jsonify(text):
    """Substitute for flask.jsonify which accepts an already encoded
    JSON string and makes a response with 200 status and application/json
    Content-Type header.
    """
    return make_response(text, 200, {'Content-Type': 'application/json'})


@app.route('/ws/v1/cluster/apps')
def applications():
    """Mock of the YARN cluster apps REST resource."""
    if 'last' in request.args:
        return jsonify(redis.get(request.base_url))

    d = st.fixed_dictionaries({
        'allocatedMB': st.integers(-1),
        'allocatedVCores': st.integers(-1),
        'amContainerLogs': st.text(),
        'amHostHttpAddress': st.text(),
        'applicationTags': st.text(),
        'applicationType': st.sampled_from(['MAPREDUCE', 'SPARK']),
        'clusterId': st.integers(0),
        'diagnostics': st.text(),
        'elapsedTime': st.integers(0),
        'finalStatus': st.sampled_from(['UNDEFINED', 'SUCCEEDED', 'FAILED', 'KILLED']),
        'finishedTime': st.integers(0),
        'id': st.text(string.ascii_letters, min_size=5, max_size=25),
        'memorySeconds': st.integers(0),
        'name': st.text(min_size=5),
        'numAMContainerPreempted': st.integers(0),
        'numNonAMContainerPreempted': st.integers(0),
        'preemptedResourceMB': st.integers(0),
        'preemptedResourceVCores': st.integers(0),
        'progress': st.floats(0, 100),
        'queue': st.text(),
        'runningContainers': st.integers(-1),
        'startedTime': st.integers(0),
        'state': st.sampled_from(['NEW', 'NEW_SAVING', 'SUBMITTED', 'ACCEPTED', 'RUNNING', 'FINISHED', 'FAILED', 'KILLED']),
        'trackingUI': st.text(),
        'trackingUrl': st.just(os.environ['YARN_ENDPOINT']),
        'user': st.text(),
        'vcoreSeconds': st.integers(0)
    })

    result = json.dumps({
        'apps': {
            'app': st.lists(d, min_size=1, average_size=5).example()
        }
    })
    redis.set(request.base_url, result)
    return jsonify(result)


@app.route('/ws/v1/cluster/metrics')
def metrics():
    """Mock of the YARN cluster metrics REST resource."""
    if 'last' in request.args:
        return jsonify(redis.get(request.base_url))

    d = st.fixed_dictionaries({
        'activeNodes': st.integers(0),
        'allocatedMB': st.integers(0),
        'allocatedVirtualCores': st.integers(0),
        'appsCompleted': st.integers(0),
        'appsFailed': st.integers(0),
        'appsKilled': st.integers(0),
        'appsPending': st.integers(0),
        'appsRunning': st.integers(0),
        'appsSubmitted': st.integers(0),
        'availableMB': st.integers(0),
        'availableVirtualCores': st.integers(0),
        'containersAllocated': st.integers(0),
        'containersPending': st.integers(0),
        'containersReserved': st.integers(0),
        'decommissionedNodes': st.integers(0),
        'lostNodes': st.integers(0),
        'rebootedNodes': st.integers(0),
        'reservedMB': st.integers(0),
        'reservedVirtualCores': st.integers(0),
        'totalMB': st.integers(0),
        'totalNodes': st.integers(0),
        'totalVirtualCores': st.integers(0),
        'unhealthyNodes': st.integers(0)
    })
    result = json.dumps({
        'clusterMetrics': d.example()
    })
    redis.set(request.base_url, result)
    return jsonify(result)


@app.route('/api/v1/applications/<app_id>/jobs')
def spark_application(app_id):
    """Mock of the Spark jobs REST resource."""
    if 'last' in request.args:
        return jsonify(redis.get(request.base_url))

    d = st.fixed_dictionaries({
        'jobId': st.integers(0),
        'name': st.text(),
        'submissionTime': st.text(),
        'completionTime': st.text(),
        'stageIds': st.lists(st.integers(0), average_size=3),
        'status': st.sampled_from(['SUCCEEDED', 'RUNNING', 'FAILED']),
        'numTasks': st.integers(0),
        'numActiveTasks': st.integers(0),
        'numCompletedTasks': st.integers(0),
        'numSkippedTasks': st.integers(0),
        'numFailedTasks': st.integers(0),
        'numActiveStages': st.integers(0),
        'numCompletedStages': st.integers(0),
        'numSkippedStages': st.integers(0),
        'numFailedStages': st.integers(0),
    })
    result = json.dumps(st.lists(d, average_size=3).example())
    redis.set(request.base_url, result)
    return jsonify(result)


@app.route('/ws/v1/mapreduce/jobs')
def mapreduce_application():
    """Mock of the mapreduce jobs REST resource."""
    if 'last' in request.args:
        return jsonify(redis.get(request.base_url))

    d = st.fixed_dictionaries({
        'startTime': st.integers(0),
        'finishTime': st.integers(0),
        'elapsedTime': st.integers(0),
        'id': st.integers(0),
        'name': st.text(),
        'user': st.text(),
        'state': st.sampled_from(['NEW', 'SUCCEEDED', 'RUNNING', 'FAILED', 'KILLED']),
        'mapsTotal': st.integers(0),
        'mapsCompleted': st.integers(0),
        'reducesTotal': st.integers(0),
        'reducesCompleted': st.integers(0),
        'mapProgress': st.floats(0, 100),
        'reduceProgress': st.floats(0, 100),
        'mapsPending': st.integers(0),
        'mapsRunning': st.integers(0),
        'reducesPending': st.integers(0),
        'reducesRunning': st.integers(0),
        'uberized': st.booleans(),
        'diagnostics': st.text(),
        'newReduceAttempts': st.integers(0),
        'runningReduceAttempts': st.integers(0),
        'failedReduceAttempts': st.integers(0),
        'killedReduceAttempts': st.integers(0),
        'successfulReduceAttempts': st.integers(0),
        'newMapAttempts': st.integers(0),
        'runningMapAttempts': st.integers(0),
        'failedMapAttempts': st.integers(0),
        'killedMapAttempts': st.integers(0),
        'successfulMapAttempts': st.integers(0)
    })
    result = json.dumps({
        'jobs': {
            'job': st.lists(d, average_size=3).example()
        }
    })
    redis.set(request.base_url, result)
    return jsonify(result)