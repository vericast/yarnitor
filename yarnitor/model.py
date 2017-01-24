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
    """Model class that exposes various yarn metrics.

    Backed by redis.

    """

    @property
    @cache.cached(timeout=2, key_prefix="yarnmodel.state")
    def state(self):
        return json.loads(redis_store.get(YARN_STATUS_KEY))

    def applications(self):
        print(self)
        print("ALL_INFO {}".format(str(self.state)[:80]))
        return self.state["current"]

    def application_info(self, application_id):
        return self.state["current"].get(application_id, {})

    def cluster_metrics(self):
        return self.state["cluster-metrics"]
