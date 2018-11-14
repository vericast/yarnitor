"""YARN monitoring RESTful API."""
from flask import jsonify, Blueprint


api_bp = Blueprint('api', __name__)


def get_model(cluster):
    """Gets a YARNModel for a given cluster

    Returns
    -------
    YARNModel
    """
    from .model import get_model
    return get_model(cluster)


@api_bp.route('/api/clusters/<cluster>')
def get_cluster(cluster):
    """Gets cluster-level metrics.

    Parameters
    ----------
    cluster: str
        Cluster identifier corresponding with a YARNModel key to find info
        about the cluster

    Returns
    -------
    JSON str
    """
    ym = get_model(cluster)
    return jsonify({"data": [ym.cluster_metrics()]})


@api_bp.route('/api/clusters/<cluster>/status')
def status(cluster):
    """Gets cluster-level metrics.

    Parameters
    ----------
    cluster: str
        Cluster identifier corresponding with a YARNModel key to find info
        about the cluster

    Returns
    -------
    JSON str
    """
    ym = get_model(cluster)
    return jsonify({
        'status': 'ok' if ym.exists() else 'unknown',
        'refresh_datetime': ym.refresh_datetime(),
        'current_rm': ym.current_rm()
    })


@api_bp.route('/api/clusters/<cluster>/applications')
def get_applications(cluster):
    """Gets information about all YARN applications.

    Parameters
    ----------
    cluster: str
        Cluster identifier corresponding with a YARNModel key to find info
        about the cluster

    Returns
    -------
    JSON str
    """
    ym = get_model(cluster)
    data = [value for value in ym.applications().values()]
    return jsonify({'data': data})


@api_bp.route('/api/clusters/<cluster>/applications/<app_id>')
def get_application(cluster, app_id):
    """Gets information about a single YARN application.

    Parameters
    ----------
    cluster: str
        Cluster identifier corresponding with a YARNModel key to find info
        about the cluster
    app_id: str
        YARN application identifier

    Returns
    -------
    JSON str
    """
    ym = get_model(cluster)
    return jsonify({"data": [ym.application_info(application_id=app_id)]})
