"""YARN monitoring RESTful API."""

from flask import jsonify, Blueprint

api_bp = Blueprint('api', __name__)


def get_model():
    """Get the YARNModel that we are going to use to retrieve information.

    Returns
    -------
    YARNModel

    """
    from .model import YARNModel
    return YARNModel()


@api_bp.route('/api/status')
def status():
    return jsonify({'status': 'ok'})


@api_bp.route('/api/applications')
def get_applications():
    ym = get_model()
    data = [value for value in ym.applications().values()]
    return jsonify({'data': data})


@api_bp.route('/api/applications/:app_id')
def get_application(app_id):
    ym = get_model()
    return jsonify({"data": [ym.application_info(application_id=app_id)]})


@api_bp.route('/api/cluster')
def get_cluster():
    ym = get_model()
    return jsonify({"data": [ym.cluster_metrics()["clusterMetrics"]]})
