from flask import Flask, render_template, send_from_directory, session
import json, sys

app = Flask(__name__)

data = open(sys.argv[1], "r").read()

@app.route("/data")
def get_data():
    return data

@app.route("/")
def main():
    return render_template("sense-dashboard.html",
        js_deps=["jquery.min.js", "d3.min.js", "sense.js", "bootstrap.min.js"],
        css_deps=["bootstrap.min.css"]
    )

@app.route("/s/<path:filename>")
def base_static(filename):
    return send_from_directory(app.root_path + '/static/', filename)

@app.context_processor
def provide_constants():
    return {
      "site_title": "Sense -- Disk Analysis"
    }


if __name__=="__main__":
  app.run()
