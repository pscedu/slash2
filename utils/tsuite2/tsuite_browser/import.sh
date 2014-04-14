#!/bin/bash

mongoimport --jsonArray -c tsets --drop example_db.json --db tsuite_browser
