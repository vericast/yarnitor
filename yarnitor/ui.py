"""YARN monitoring built-in UI."""
import time

from flask import Blueprint, abort, current_app, render_template, url_for

ui_bp = Blueprint('ui', __name__, static_folder='static')
version = str(time.time())


def get_model(cluster):
    """Gets a YARNModel for a given cluster

    Returns
    -------
    YARNModel
    """
    from .model import get_model
    return get_model(cluster)


def versioned_url_for(endpoint, **args):
    """Inserts a query string `q` into the args dictionary
    with the version string generated at module import time.

    Passes the endpoint and modified args to the stock Flask
    `url_for` function.

    Returns
    -------
    str
        Result of Flask.url_for
    """
    args['v'] = version
    return url_for(endpoint, **args)


@ui_bp.context_processor
def override_url_for():
    """Overrides `url_for` in templates to append a version
    to the query string for cache busting purposes on each
    restart of the server.

    Returns
    -------
    dict
        With versioned_url_for function assigned to key url_for
    """
    return dict(url_for=versioned_url_for)


@ui_bp.route('/', defaults={'cluster': None})
@ui_bp.route('/<cluster>')
def index(cluster):
    """Renders the YARNitor single-page app.

    Parameters
    ----------
    cluster: str
        Cluster name or blank to use the default
    """
    if cluster is None:
        cluster = current_app.config['DEFAULT_CLUSTER_KEY']
    if get_model(cluster).exists():
        return render_template('index.html', cluster=cluster)
    else:
        abort(404)
