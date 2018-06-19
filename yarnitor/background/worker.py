"""
Fetches information about YARN applications from the YARN HTTP API.

Uses that base information to fetch additional details if the application
is a Spark application or MapReduce application. Requires the following
environment variables to configure itself.

YARN_ENDPOINT
    host:port for yarn api
YARN_POLL_SLEEP
    time to sleep between polling in seconds
REDIS_ENDPOINT
    host:port for a redis instance
LOG_LEVEL
    DEBUG, INFO (default), WARNING, ERROR strings from the logging package
"""

import atexit
import concurrent.futures
import datetime
import json
import logging
import os
import time

from urllib.parse import urlparse

import redis
import requests

from yarnitor.common_config import YARN_STATUS_KEY

logger = logging.getLogger("yarn-background-worker")

# Number of workers in the threadpool
THREADPOOL_SIZE = 16
# Timeout for fetching results using the threadpool
THREADPOOL_TIMEOUT = 120
# Sentinel state used when we fail to query the application for its state
NON_RESPONSIVE_STATE = 'NON_RESPONSIVE'
# The YARN API responds with a Redirect header when the configured
# YARN ResrouceManager host is no longer the primary. The YARNHandler class
# knows how to interpret this redirect. Because distributed systems are fun,
# the primary may change yet again before the YARNHandler makes the next
# request resulting in yet another redirect. This constant represents the
# maximum number of redirects the YARNHandler should follow in a single
# update attempt, after which it will wait until the next update interval to
# try again. This cap avoids the potential for endless, busy loop
# ping-ponging among YARN ResourceManagers.
MAX_HA_REDIRECTS = 5

# Global threadpool for running async tasks
threadpool = concurrent.futures.ThreadPoolExecutor(THREADPOOL_SIZE)
# Shut down the pool when the process is exiting, passing False to
# avoid waiting in this lambda for all tasks to complete.
atexit.register(lambda: threadpool.shutdown(False))

class Progress(object):
    """Utility class for storing mutable progress information."""
    def __init__(self, name, completed=0, failed=0, running=0, total=0):
        self.name = name
        self.completed = completed
        self.failed = failed
        self.running = running
        self.total = total

    def to_dict(self):
        return self.__dict__


class YARNHandler(object):
    """Manages HTTP communication with a YARN ResourceManager (RM) to fetch
    information about applications.

    Parameters
    ----------
    host: str
        Protocol, hostname, and optional port of the YARN RM. Can be a comma
        separated list of values in which case the first is chosen as the
        primary RM and the others are used if the primary becomes unavailable.
    version: str, optional
        Version of the YARN RM API

    Attributes
    ----------
    base_url: dict
        'host' and 'version' of the YARN RM
    """
    def __init__(self, host, version="v1"):
        self.all_hosts = host.split(',')
        self.base_url = {
            'host': self.all_hosts[0],
            'version': version
        }

    def get_url(self, path, **params):
        """Issues an HTTP GET to the given path with the given parameters
        and treats the response as JSON.

        Parameters
        ----------
        path: str
            Path to append to the root YARN RM path
        **params, dict
            Query parameters to append to the YARN RM URL

        Returns
        -------
        dict
            JSON decoded response
        """
        available_hosts = set(self.all_hosts)
        # Handle HA redirects in a loop so that we can control how many
        # we're willing to follow.
        for i in range(MAX_HA_REDIRECTS):
            final_url = "{host}/ws/{version}/{path}".format(path=path, **self.base_url)
            resp = requests.get(final_url, params)
            if resp.status_code >= 400:
                # Take the host out of the pool of available for this attempt only
                available_hosts.remove(self.base_url['host'])
                if not available_hosts:
                    resp.raise_for_status()
                # Take one, any one, as the new primary
                for new_host in self.all_hosts: break
                self.base_url['host'] = new_host
                logger.warn('YARN RM down, switching to URL: %s', new_host)
            else:
                # YARN sometimes uses the Refresh header to indicate a change in the primary RM
                ha_redirect = resp.headers.get('Refresh')
                if ha_redirect is None:
                    # The configured RM is still the primary RM: leave it be
                    break
                # Store the new host in the instance for the next request
                # It comes in the form "3; url=http://newhost:port/same/path/as/request?args"
                _, key = ha_redirect.split(';')
                _, new_url = key.strip().split('url=')
                parsed = urlparse(new_url.strip())
                new_host = '{parsed.scheme}://{parsed.netloc}'.format(parsed=parsed)
                self.base_url['host'] = new_host
                logger.warn('YARN RM redirect, switching to URL: %s', new_host)
        else:
            raise RuntimeError('Too many YARN redirects')
        return resp.json()

    def cluster_applications(self, *state):
        """Gets information about YARN apps with the given state.

        Parameters
        ----------
        *state: str, optional
            Request applications with the given string state(s) only.
            Empty means apps with any state will be included in the response.

        Returns
        -------
        dict
            JSON decoded response from the YARN RM
        """
        return self.get_url("cluster/apps", state=','.join(state))

    def cluster_metrics(self):
        """Gest information about the YARN cluster.

        Returns
        -------
        dict
            JSON decoded response from the YARN RM
        """
        return self.get_url("cluster/metrics")


class BaseHandler(object):
    """Base handler for transforming info about a YARN application
    into the structure and detail the frontend expects.

    Parameters
    ----------
    tracking_url: str
        URL of the server responsible for tracking the status of the YARN app
    application_id: str
        Unique ID of the YARN application
    """
    def __init__(self, tracking_url, application_id):
        self.tracking_url = tracking_url.rstrip("/")
        self.application_id = application_id

    def get_url(self, path, **params):
        """Issues an HTTP GET against the given path on the app tracking server with
        the given parameters and treats the response as JSON.

        Parameters
        ----------
        path: str
            Path to append to the root YARN RM path
        **params: dict
            Query parameters to append to the YARN RM URL

        Returns
        -------
        dict
            JSON decoded response
        """
        url = '{tracking_url}/{path}'.format(tracking_url=self.tracking_url, path=path)
        # Under some security models, the YARN proxy requires that a user click a link in
        # order to access the tracking URL. Setting a cookie has the same effect, programmatically.
        cookies = {"checked_{}".format(self.application_id): 'true'}
        resp = requests.get(url, params, cookies=cookies, timeout=10)
        return resp.json()

    def generate_standardized_info(self, yarn_application_info):
        """Transforms information from the YARN ResourceManager and the YARN ApplicationMaster
        into a dictionary of standard fields used by the frontend.

        Parameters
        ----------
        yarn_application_info: dict
            Information about a single app retrieved using YARNHandler.cluster_applications()

        Returns
        -------
        dict
            Fields to copy verbatim from the YARN information about the app plus an
            empty 'progress' list to be populated by more specific subtypes
        """
        verbatim_fields = ["id", "name", "user", "applicationType", "queue",
                           "startedTime", "allocatedMB", "allocatedVCores",
                           "trackingUrl", "state", "memorySeconds",
                           "vcoreSeconds"]
        r = {k: yarn_application_info[k] for k in verbatim_fields}
        r["progress"] = []
        return r


class SparkHandler(BaseHandler):
    """Aggregates Spark job information for the frontend."""
    def get_jobs(self, status=None):
        """Issues an HTTP GET to fetch information about Spark jobs.

        Parameters
        ----------
        status: str, optional
            Request jobs with this status only; None means all jobs

        Returns
        -------
        dict
            JSON-decoded response, https://spark.apache.org/docs/latest/monitoring.html#rest-api
        """
        path = "api/v1/applications/{id}/jobs".format(id=self.application_id)
        params = {"status": status} if status is not None else {}
        return self.get_url(path, **params)

    def _aggregate_tasks(self, name, tasks):
        """Aggregates the task metrics for a job.

        Parameters
        ----------
        name: str
            Name describing the tasks
        tasks: list
            List of task info dictionaries from the Spark tracking API

        Returns
        -------
        dict
            Progress object in dictionary form
        """
        p = Progress(name)
        for jobinfo in tasks:
            p.completed += jobinfo["numCompletedTasks"]
            p.failed += jobinfo["numFailedTasks"]
            p.running += jobinfo["numActiveTasks"]
            p.total += jobinfo["numTasks"]
        return p.to_dict()

    def generate_standardized_info(self, yarn_application_info):
        """Transforms information from the YARN ResourceManager and the Spark ApplicationMaster
        into a dictionary of standard fields used by the frontend.

        Parameters
        ----------
        yarn_application_info: dict
            Information about a single app retrieved using YARNHandler.cluster_applications()

        Returns
        -------
        dict
            Fields to copy verbatim from the YARN information about the app plus a
            'progress' list of dictionaries about Running and Total tasks
        """
        r = super().generate_standardized_info(yarn_application_info)
        jobs = self.get_jobs("running")
        if jobs:
            r["progress"].append(self._aggregate_tasks("Running Tasks", jobs))
            r["state"] = "RUNNING"
        else:
            r["state"] = "IDLE"
            r["progress"].append(Progress("Running Tasks").to_dict())

        all_jobs = self.get_jobs()
        r["progress"].append(self._aggregate_tasks("Total", all_jobs))

        return r


class MapredHandler(BaseHandler):
    """Aggregates MapReduce job information for the frontend."""
    def get_jobs(self):
        """Issues an HTTP GET to fetch information about MapReduce jobs.

        Returns
        -------
        dict
            JSON-decoded response, https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapredAppMasterRest.html#Job_API
        """
        return self.get_url('ws/v1/mapreduce/jobs')

    def _aggregate_maps(self, tasks):
        """Aggregates the mapper metrics for a job.

        Parameters
        ----------
        name: str
            Name describing the mappers
        tasks: list
            List of mapper info dictionaries from the MR tracking API

        Returns
        -------
        dict
            Progress object in dictionary form
        """
        p = Progress("Maps")
        for jobinfo in tasks:
            p.completed += jobinfo["mapsCompleted"]
            p.failed += jobinfo["failedMapAttempts"]
            p.running += jobinfo["mapsRunning"]
            p.total += jobinfo["mapsTotal"]
        return p.to_dict()

    def _aggregate_reduces(self, tasks):
        """Aggregates the reducer metrics for a job.

        Paramters
        ---------
        name: str
            Name describing the reducers
        tasks: list
            List of reducers info dictionaries from the MR tracking API

        Returns
        -------
        dict
            Progress object in dictionary form
        """
        p = Progress("Reduces")
        for jobinfo in tasks:
            p.completed += jobinfo["reducesCompleted"]
            p.failed += jobinfo["failedReduceAttempts"]
            p.running += jobinfo["reducesRunning"]
            p.total += jobinfo["reducesTotal"]
        return p.to_dict()

    def generate_standardized_info(self, yarn_application_info):
        """Transforms information from the YARN ResourceManager and the MapReduce ApplicationMaster
        into a dictionary of standard fields used by the frontend.

        Parameters
        ----------
        yarn_application_info: dict
            Information about a single app retrieved using YARNHandler.cluster_applications()

        Returns
        -------
        dict
            Fields to copy verbatim from the YARN information about the app plus a
            'progress' list of dictionaries about Map and Reduce tasks
        """
        r = super().generate_standardized_info(yarn_application_info)
        jobs = self.get_jobs().get("jobs", {}).get("job", [])
        if jobs:
            r['progress'] = []
            r['progress'].append(self._aggregate_maps(jobs))
            r['progress'].append(self._aggregate_reduces(jobs))

        return r


class YARNPoller(object):
    """Polls a YARN ResourceManager and YARN ApplicationMasters for information
    about the state of known applications.

    Parameters
    ----------
    redis_client: redis.StrictRedis
        Used to stash state from the last fetch in redis for use by the frontend
    yarn_handler: YARNHandler
        Used to issue HTTP requests to a YARN ResourceManager

    Attributes
    ----------
    redis_client: redis.StrictRedis
        Used to stash state from the last fetch in redis for use by the frontend
    yarn_handler: YARNHandler
        Used to issue HTTP requests to a YARN ResourceManager
    application_handlers: dict
        Maps applicationType to BaseHandler-derived classes that can fetch
        additional information about applications
    state: dict
        Last known cluster application state
    """
    def __init__(self, redis_client, yarn_handler):
        self.redis_client = redis_client
        self.yarn_handler = yarn_handler
        self.application_handlers = {}
        self.state = {"current": {}, "cluster-metrics": {}}

    def register_handler(self, application_type, handler_class):
        """Registers a BaseHandler class to handle fetching progress details
        about a specific application type.

        Parameters
        ----------
        application_type : str
            Application type to handle with the given class
        handler_class : BaseHandler
            Subclass of BaseApplicationInfo
        """
        self.application_handlers[application_type] = handler_class

    def _make_application_handler(self, yarn_application_info):
        """Instantiates a handler for the given YARN application info
        based on its applicationType field value and the registered handlers.

        Falls back on using a BaseHandler instance for basic metrics when there
        is no handler registered for the type in the response.

        Parameters
        ----------
        yarn_application_info: dict
            Information about a single app retrieved using YARNHandler.cluster_applications()

        Returns
        -------
        instance of BaseHandler

        Raises
        ------
        KeyError
            When the response from YARN does not contain the expected app structure
        """
        app_type = yarn_application_info['applicationType']
        klass = self.application_handlers.get(app_type, BaseHandler)
        return klass(yarn_application_info['trackingUrl'], yarn_application_info['id'])

    def _generate_listing(self):
        """Computes the listing of YARN applications and the additional information
        provided by the handlers.

        Blocks while running the work of fetching application details from tracking
        APIs using the global threadpool.

        Returns
        -------
        dict
            YARN application IDs as keys mapped to application detail dictionaries
            as values
        """
        # Fetch all running applications
        cluster_apps = self.yarn_handler.cluster_applications("RUNNING")
        if 'apps' not in cluster_apps or cluster_apps['apps'] is None:
            # Something might be wrong if there are no applications
            logger.warn('No application data available')
            return {}
        apps = cluster_apps['apps']['app']

        def run_task(app):
            """Fetches application details using the appropriate registered
            handler for the application type.

            Parameters
            ----------
            app: dict
                Application information from the YARN ResourceManager

            Returns
            -------
            dict
                Application information in the format expected by the frontend
            """
            std_info = None
            try:
                ah = self._make_application_handler(app)
                std_info = ah.generate_standardized_info(app)
            except Exception as ex:
                # For now, we log all exceptions as errors, but we should
                # be more selective about what we catch and handle as a
                # passing warning versus let bubble because it's a real problem
                logger.exception("Error for application %s %s", app["id"],
                                 app["name"])
            # Fall back to just the yarn information
            if std_info is None:
                ah = BaseHandler(app['trackingUrl'], app['id'])
                std_info = ah.generate_standardized_info(app)
                # Indicate that the tracking API for the app did not respond
                std_info["state"] = NON_RESPONSIVE_STATE

            return std_info

        # Wait for all async results, raise if it takes too long
        async_result = threadpool.map(run_task, apps, timeout=THREADPOOL_TIMEOUT)
        # Materialize results as a list, we need them all anyway
        results = list(async_result)
        # Count the number of apps with the non-responsive state set
        num_unknown_state = sum(1 if info['state'] == NON_RESPONSIVE_STATE else 0
                                for info in results)

        # If all of the applications are non-responsive, then it's quite possible
        # the YARN proxy is down and the true state of everything should be unknown,
        # not unresponsive which suggests an app problem
        if num_unknown_state == len(results):
            for result in results:
                result['state'] = 'UNKNOWN'

        # Key all results by the app id
        result = {info["id"]: info for info in results}
        logger.debug("Update {}: Result: {}...".format(self, str(result)[:80]))

        return result

    def run_update(self):
        """Fetches YARN cluster and application information, and store it timestamped
        in redis as a JSON string for retrieval by the frontend.
        """
        logger.info("Updating metrics from YARN")
        self.state["current"] = self._generate_listing()
        self.state["cluster-metrics"] = self.yarn_handler.cluster_metrics()
        # Make the datetime conform to true ISO-8601 by adding Z(ulu) to indicate
        # this is truly a UTC time (without a timezone, the spec says it should be
        # treated as local time which is definitely NOT what we want)
        # https://en.wikipedia.org/wiki/ISO_8601#Time_zone_designators
        self.state["refresh-datetime"] = datetime.datetime.utcnow().isoformat() + 'Z'
        self.redis_client.set(YARN_STATUS_KEY, json.dumps(self.state))
        logger.info("Done updating metrics from YARN")

    def loop(self, sleep_time):
        """Executes the `run_update` message in a loop that catches and logs
        all exceptions indefinitely.

        Parameters
        ----------
        sleep_time: float
            Time to sleep after executing `run_update`
        """
        while True:
            try:
                self.run_update()
            except Exception:
                logger.exception('Unknown exception while updating')
            time.sleep(sleep_time)


def main():
    """Creates a redis client, a YARN ResourceManager REST API client, and a YARN
    poller that puts information about the YARN cluster and its applications into
    redis on a timed interval.
    """
    log_level = os.getenv('LOG_LEVEL', 'INFO')
    logging.basicConfig(level=getattr(logging, log_level))

    host, port = os.environ['REDIS_ENDPOINT'].split(":")
    redis_client = redis.StrictRedis(host=host, port=port)
    yarn_handler = YARNHandler(os.environ['YARN_ENDPOINT'])

    ym = YARNPoller(redis_client, yarn_handler)
    ym.register_handler("SPARK", SparkHandler)
    ym.register_handler("MAPREDUCE", MapredHandler)
    ym.register_handler("MAPRED", MapredHandler)
    ym.loop(int(os.environ["YARN_POLL_SLEEP"]))


if __name__ == '__main__':
    main()
