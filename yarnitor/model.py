"""YARN monitoring model."""
from flask import json

from .core import redis_store, cache
from .common_config import YARN_STATUS_KEY


class Singleton(type):
    """Metaclass for making a singleton."""

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class YARNModel(object, metaclass=Singleton):
    """Model class that exposes YARN metrics stored in redis
    by a separate worker process.
    """
    @property
    @cache.cached(timeout=2, key_prefix="yarnmodel.state")
    def state(self):
        state = redis_store.get(YARN_STATUS_KEY)
        return json.loads(state) if state is not None else {}

    def refresh_datetime(self):
        """Get the UTC datetime of the last data fetch.

        Returns
        =======
        str
            ISO-8601 datetime string
        """
        return self.state.get("refresh-datetime", '')

    def applications(self):
        return self.state.get("current", {})

    def application_info(self, application_id):
        return self.state.get("current", {}).get(application_id, {})

    def cluster_metrics(self):
        metrics = self.state.get("cluster-metrics", {})
        return metrics.get('clusterMetrics', {})
