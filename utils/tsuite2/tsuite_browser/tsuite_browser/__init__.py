from flask import Flask, render_template, send_from_directory, session

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

@app.route("/api/tsets", methods=["GET"])
@return_json
def get_tsets():
    return list(api.get_tsets())

@app.route("/l")
def logout():
    session.clear()
    return "ok"

@app.route("/")
@app.route("/<int:tsid>")
def dashboard(tsid = None):
    if not tsid:
        latest = api.get_latest_tset()
        session["active_tsid"] = latest["tsid"]
    else:
        session["active_tsid"] = tsid



    return render_template("new.html",
        tsets = api.get_tsets(100),
        display_tset = api.get_tset_display(session["active_tsid"]),
        adj_tests = api.get_neighboring_tests(session["active_tsid"], 0)
    )

@app.route('/s/<path:filename>')
def base_static(filename):
    return send_from_directory(app.root_path + '/static/', filename)

@app.context_processor
def provide_constants():
    return {
      "site_title": "SLASH2 - Test Suite Result Browser",
      "nav_title" : "TSuite Browser"
    }

