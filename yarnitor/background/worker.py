"""
Background processing script for polling yarn and getting things from it.

Environment Variables
=====================
YARN_ENDPOINT
    host:port for yarn api
YARN_POLL_SLEEP
    time to sleep between polling in seconds
REDIS_ENDPOINT :
    host:port for a redis instance
"""

import atexit
import concurrent.futures
import logging
import os
import time

import redis
import requests
from flask import json

from yarnitor.common_config import YARN_STATUS_KEY

host, port = os.getenv('REDIS_ENDPOINT', "localhost:6379").split(":")
redis_client = redis.StrictRedis(host=host, port=port)

logger = logging.getLogger("yarn-background-worker")


class Progress(object):
    """Utility class for storing progress information
    """

    def __init__(self, name, completed=0, failed=0, running=0, total=0):
        self.name = name
        self.completed = completed
        self.failed = failed
        self.running = running
        self.total = total


class YarnApi(object):
    """Collection of yarn endpoints we care about.
    """

    def __init__(self, host, version="v1"):
        self.host = host
        self.version = version

    def get_url(self, url, **params):
        final_url = "http://{host}/ws/{version}".format(**self.__dict__) + url
        resp = requests.get(final_url, params)
        return resp.json()

    def cluster_applications(self, state):
        return self.get_url("/cluster/apps", state=state)

    def cluster_application(self, application_id):
        return self.get_url("/cluster/apps/{}".format(application_id))

    def cluster_metrics(self):
        return self.get_url("/cluster/metrics")


class BaseHandler(object):
    """Basic handler class providing the scaffolding that the rest of the
    library expects.

    """
    prefix = ""
    version = "v1"

    def __init__(self, tracking_url, application_id):
        self.tracking_url = tracking_url.rstrip("/")
        self.application_id = application_id

    @classmethod
    def from_yarn_application_info(cls, info):
        """Alternate constructor creating an instance from the output of a yarn
        application listing

        Parameters
        ----------
        info : dict

        Returns
        -------
        cls
        """
        return cls(tracking_url=info["trackingUrl"], application_id=info["id"])

    def get_url(self, url, **params):
        """Retrieves a url, params set and converts the result to a python dict

        Parameters
        ----------
        url : string
        params : dict

        Returns
        -------
        dict
        """
        final_url = (self.prefix + url).format(version=self.version, **self.__dict__)
        # Under some security models for YARN going to the tracking url
        # required clicking through a security prompt.
        # This presets that cookie.
        cookies = {"checked_{}".format(self.application_id): 'true'}
        resp = requests.get(final_url, params, cookies=cookies, timeout=10)
        return resp.json()

    def generate_standardized_info(self, yarn_application_info):
        """Generates the standardized dictionary of fields that are expected by
        listing applications.

        Parameters
        ----------
        yarn_application_info

        Returns
        -------
        dict
        """
        verbatim_fields = ["id", "name", "user", "applicationType", "queue",
                           "startedTime", "allocatedMB", "allocatedVCores",
                           "trackingUrl", "state", "memorySeconds",
                           "vcoreSeconds"]

        r = {k: yarn_application_info[k] for k in verbatim_fields}
        # defaults
        r["job"] = "1"

        # Progress should contain various progress records formatted as
        # follows.
        # This is the fallback progress handler for when we do not have a
        # handler for the specific yarn application
        # type.
        r["progress"] = [
            {
                "name": "yarn-progress",
                "completed": int(yarn_application_info["progress"]),
                "failed": 0,
                "running": 0,
                "total": 100
            }
        ]

        r["type_specific"] = {}
        return r


class SparkHandler(BaseHandler):
    prefix = "{tracking_url}/api/{version}"

    def jobs(self, job_id=None, status=None):
        base = "/applications/{application_id}/jobs"
        if job_id:
            base += "/{}".format(job_id)
        if status:
            params = {"status": status}
        else:
            params = {}
        return self.get_url(base, **params)

    def _aggregate_tasks(self, name, tasks):
        p = Progress(name)
        for jobinfo in tasks:
            p.completed += jobinfo["numCompletedTasks"]
            p.failed += jobinfo["numFailedTasks"]
            p.running += jobinfo["numActiveTasks"]
            p.total += jobinfo["numTasks"]
        return p.__dict__

    def generate_standardized_info(self, yarn_application_info):
        r = super().generate_standardized_info(yarn_application_info)

        all_jobs = self.jobs()
        r["job"] = max((j["jobId"] for j in all_jobs), default=0)

        r["progress"] = []

        jobs = self.jobs(status="running")
        if len(jobs) > 0:
            r["progress"].append(self._aggregate_tasks("Running Tasks", jobs))
            r["state"] = "RUNNING"
            r["job"] = max(j["jobId"] for j in jobs)
        else:
            r["state"] = "IDLE"
            r["progress"].append(Progress("Running Tasks").__dict__)

        r["progress"].append(self._aggregate_tasks("Total", all_jobs))

        return r


class MapredHandler(BaseHandler):
    prefix = "{tracking_url}/ws/{version}/mapreduce"

    def jobs(self, job_id=None):
        base = "/jobs"
        return self.get_url(base)

    def _aggregate_maps(self, tasks):
        p = Progress("Map")
        for jobinfo in tasks:
            p.completed += jobinfo["mapsCompleted"]
            p.failed += jobinfo["failedMapAttempts"]
            p.running += jobinfo["mapsRunning"]
            p.total += jobinfo["mapsTotal"]
        return p.__dict__

    def _aggregate_reduces(self, tasks):
        p = Progress("Reduces")
        for jobinfo in tasks:
            p.completed += jobinfo["reducesCompleted"]
            p.failed += jobinfo["failedReduceAttempts"]
            p.running += jobinfo["reducesRunning"]
            p.total += jobinfo["reducesTotal"]
        return p.__dict__

    def generate_standardized_info(self, yarn_application_info):
        r = super().generate_standardized_info(yarn_application_info)
        jobs = self.jobs().get("jobs", {}).get("job", [])
        if len(jobs) > 0:
            r['progress'] = []
            r['progress'].append(self._aggregate_maps(jobs))
            r['progress'].append(self._aggregate_reduces(jobs))

        return r


threadpool = concurrent.futures.ThreadPoolExecutor(16)
atexit.register(lambda: threadpool.shutdown(False))


class Singleton(type):
    """Metaclass for making a singleton."""

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class YARNModel(object, metaclass=Singleton):
    """TODO: replace me"""

    def __init__(self):
        self.yarn_handler = YarnApi(os.environ["YARN_ENDPOINT"])
        self.sleep_time = int(os.environ["YARN_POLL_SLEEP"])
        self.application_handlers = {}
        self.register_handler("SPARK", SparkHandler)
        self.register_handler("MAPREDUCE", MapredHandler)
        self.register_handler("MAPRED", MapredHandler)
        self.state = {"current": {}, "cluster-metrics": {}}
        self.terminated = False
        self.background_thread = None

    def register_handler(self, application_type, handler_class):
        """

        Parameters
        ----------
        application_type : str
        handler_class : BaseHandler
            Subclass of BaseApplicationInfo

        """
        self.application_handlers[application_type] = handler_class

    def _make_application_handler(self, yarn_application_info):
        """Generates a handler for the given yarn application info.

        This allows pluggability for different kinds of applications.

        Parameters
        ----------
        yarn_application_info : dict

        Returns
        -------
        instance of BaseApplicationInfo
        """
        app_type = yarn_application_info['applicationType']
        klass = self.application_handlers.get(app_type, BaseHandler)
        return klass.from_yarn_application_info(yarn_application_info)

    def _generate_listing(self):
        """Computes the listing of applications and the additional information
        provided by the handlers.

        """
        cluster_apps = self.yarn_handler.cluster_applications("RUNNING")
        if 'apps' not in cluster_apps or cluster_apps['apps'] is None:
            logger.warn('No application data available')
            return {}
        apps = cluster_apps['apps']['app']

        def run_task(app):
            std_info = None
            try:
                ah = self._make_application_handler(app)
                std_info = ah.generate_standardized_info(app)
            # Due to timing things happening here some of our calls may fail
            except requests.exceptions.ReadTimeout:
                pass
            except Exception as ex:
                logger.error("Error for application %s %s", app["id"], app["name"])
                logger.exception(ex)
            # Falling back to just the yarn information
            if std_info is None:
                ah = BaseHandler.from_yarn_application_info(app)
                std_info = ah.generate_standardized_info(app)
                std_info["state"] = "NON_RESPONSIVE"

            return std_info

        aresult = threadpool.map(run_task, apps)
        result = {info["id"]: info for info in list(aresult)}
        logger.debug("Update {}: Result: {}...".format(self, str(result)[:80]))

        return result

    def run_update(self):
        """Single step for the update listing"""
        logger.debug("generating listing")
        self.state["current"] = self._generate_listing()
        self.state["cluster-metrics"] = self.yarn_handler.cluster_metrics()
        redis_client.set(YARN_STATUS_KEY, json.dumps(self.state))

    def loop(self):
        while True:
            try:
                self.run_update()
            except Exception as ex:
                logger.exception(ex)
            time.sleep(self.sleep_time)

    def close(self):
        self.terminated = True


def main():
    ym = YARNModel()
    ym.loop()


if __name__ == '__main__':
    main()
