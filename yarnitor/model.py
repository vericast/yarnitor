"""YARN monitoring model."""
import json

from .core import cache, redis_store

# YARN model singletons by redis/cluster key
_instances = {}


def get_model(key):
    """Gets a singleton YARNModel instance for the given key.

    Parameters
    ----------
    key: str
        Redis key / cluster name under which to store app info
    """
    if key not in _instances:
        _instances[key] = YARNModel(key)
    return _instances[key]


class YARNModel(object):
    """Model class that exposes YARN metrics stored in redis by a separate
    worker process.
    """
    def __init__(self, key):
        self.key = key

    @property
    @cache.cached(timeout=5)
    def state(self):
        state = redis_store.get(self.key)
        return json.loads(state) if state is not None else {}

    def exists(self):
        """Gets if information about the cluster exists.

        Returns
        -------
        bool
        """
        return redis_store.get(self.key) is not None

    def refresh_datetime(self):
        """Gets the UTC datetime of the last data fetch.

        Returns
        -------
        str
            ISO-8601 datetime string
        """
        return self.state.get("refresh-datetime", '')

    def current_rm(self):
        """Gets the URL of the YARN RM last queried, successfully or not.

        Returns
        -------
        str
        """
        return self.state.get("current-rm", '')

    def applications(self):
        """Gets all YARN application metrics.

        Returns
        -------
        dict
        """
        return self.state.get("application-metrics", {})

    def application_info(self, application_id):
        """Gets metrics for a single YARN application.

        Returns
        -------
        dict
        """
        return self.state.get("application-metrics", {}).get(application_id, {})

    def cluster_metrics(self):
        """Gets metrics for an entire YARN cluster.

        Returns
        -------
        dict
        """
        metrics = self.state.get("cluster-metrics", {})
        return metrics.get('clusterMetrics', {})
