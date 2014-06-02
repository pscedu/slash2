from flask import Flask, render_template, send_from_directory, request

import json, datetime
from bson.objectid import ObjectId
from functools import wraps

from flask.ext.pymongo import PyMongo

from api import API

app = Flask(__name__)

api = API(app)

app.secret_key = 'S98hSECRETFMSs*2ss.3.2'

class MongoJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        elif isinstance(obj, ObjectId):
            return unicode(obj)
        return json.JSONEncoder.default(self, obj)

Flask.json_encoder = MongoJsonEncoder

def return_json(f):
    @wraps(f)
    def wrapper(*args, **kwds):
        return json.dumps(f(*args, **kwds), cls=MongoJsonEncoder)
    return wrapper

@app.route("/api/tsets")
@return_json
def get_tsets():
    return list(api.get_tsets())

@app.route("/api/tsets/<int:tsid>", methods=["GET"])
@return_json
def get_tset(tsid = None):
    return api.get_tset(tsid)

@app.route("/api/tsets/latest", methods=["GET"])
@return_json
def get_latest_tset():
    return api.get_latest_tset()

@app.route("/api/tsets/display/<int:tsid>", methods=["GET"])
@return_json
def get_tset_display(tsid = None):
    return api.get_tset_display(tsid)

@app.route("/api/tsets/adj/<int:tsid>/<int:adj_tsets>", methods=["GET"])
@return_json
def get_adj_tsets(tsid = None, adj_tsets = None):
    return api.get_neighboring_tests(tsid, adj_tsets)

@app.route("/api/clear")
def logout():
    session.clear()
    return "Session cleared."

@app.route("/")
@app.route("/<int:tsid>")
def dashboard(tsid = None):
    if not tsid:
        tsid = api.get_latest_tset()["tsid"]
    return render_template("summary.html", active_tsid=tsid)

@app.route("/<string:test_name>")
@app.route("/<int:tsid>/<string:test_name>")
def test_summary(tsid = None, test_name = None):
    return render_template("test_summary.html",
            active_tsid=tsid, test_name = test_name)

@app.route('/s/<path:filename>')
def base_static(filename):
    return send_from_directory(app.root_path + '/static/', filename)

@app.context_processor
def provide_constants():
    return {
      "site_title": "SLASH2 - Test Suite Result Browser",
      "nav_title" : "TSuite Browser"
    }

