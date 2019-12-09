#
# Simple demo of Aerospike - Grafana Integration to display stock ticker values based on data in an Awrospike DB
#
from flask import Flask, request, jsonify, json, abort
from flask_cors import CORS, cross_origin

import aerospike
from aerospike_helpers.operations import list_operations as list_ops
import sys
import time
from datetime import datetime, timedelta
from dateutil import tz
import dateutil

app = Flask(__name__)

cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

methods = ('GET', 'POST')

metric_finders= {}
metric_readers = {}
annotation_readers = {}
panel_readers = {}

# Configure the Aerospike client (Change the hot IP address as appropriate)
config = {
  'hosts': [ ('127.0.0.1', 3000) ],
  'policies': {'batch': {'total_timeout': 5000}}
}

# Create a client object and connect it to the Aerospike cluster
try:
  client = aerospike.client(config).connect()
except:
  import sys
  print("failed to connect to the cluster with", config['hosts'])
  sys.exit(1)

print("Connected to Aerospike")

# Respond to an empty request with a message
@app.route('/', methods=methods)
@cross_origin()
def hello_world():
    print (request.headers, request.get_json())
    return 'Aerospike timeseries tick data demonstration'

# Respond to a '/search' request with the list of available ticker symbols
@app.route('/search', methods=methods)
@cross_origin()
def find_metrics():
    print (request.headers, request.get_json())
    req = request.get_json()

    metrics = [ "IBM", "WMT", "XOM", "BRK", "AMZN" ]
    return jsonify(metrics)

# Respond to the /query request via data from Aerospike
@app.route('/query', methods=methods)
@cross_origin(max_age=600)
def query_metrics():
    req = request.get_json()

# Determine the local and UTC timezones
    localTZ = tz.tzlocal()
    utcTZ = tz.tzutc()
    
# Extract the range of requested times from the request and adjust to the local timezone
    lowDate = datetime.strptime(req['range']['from'], "%Y-%m-%dT%H:%M:%S.%fZ")
    lowDate = lowDate.replace(tzinfo = utcTZ)
    lowDate = lowDate.astimezone(localTZ)
    highDate = datetime.strptime(req['range']['to'], "%Y-%m-%dT%H:%M:%S.%fZ")
    highDate = highDate.replace(tzinfo=utcTZ)
    highDate = highDate.astimezone(localTZ)
    timeDiff = highDate - lowDate

# Extract the requested ticker symbols from the request
    targetTickers = []
    for aTarget in req['targets']:
        targetTickers.append(aTarget['target'])
        
    # Setup a batch read for the range of dates
    results = []

    # If the requested timeframe is less than a day...
    if timeDiff.days < 1:
        for aTicker in targetTickers:
            key = ('test', 'tickerSet', aTicker + "-" + lowDate.strftime("%Y%m%d"))

            # Setop a List operation to ONLY retrieve the range of list values within a single record for the requested timestmnp range
            # This will greatly minimize the data that has to be sent from the server to the client.
            ops = [
                   list_ops.list_get_by_value_range('datapoints', aerospike.LIST_RETURN_VALUE, [int(lowDate.timestamp()) * 1000, aerospike.CDTWildcard()], [int(highDate.timestamp() * 1000), aerospike.CDTWildcard()])
                  ]
            _, _, as_results = client.operate(key, ops)
    
            res = {"target": aTicker}
            res["datapoints"] = []
            for bin in as_results["datapoints"]:
               tmp_bin = bin[0]
               bin[0] = float(bin[1])
               bin[1] = tmp_bin
               res["datapoints"].append(bin)
            results.append(res)

    else: 
        for aTicker in targetTickers:
            as_keys = []

            if timeDiff.days < 3:
                for i in range(0, timeDiff.days + 1):
                # If the range of days is less than 3...
                     # Then we want to retrieve the detailed ticks (with a second granularity) from Aerospike
                    key = ('test', 'tickerSet', aTicker + "-" + (lowDate + timedelta(days=i)).strftime("%Y%m%d"))
                    as_keys.append(key)
            else: # For >= 3 days, get a record for each year with that year's closing daily prices
                 print ("Low and High year: " + str(lowDate.year) + " " + str(highDate.year))
                 for aYear in range(lowDate.year, highDate.year + 1):
                     print ("Key Value")
                     print (aTicker + "-" + str(aYear))
                     key = ('test', 'tickerSet', aTicker + "-" + str(aYear))
                # Append to the list of keys
                     as_keys.append(key)
             
            # This is the Aerospike call that will retrieve the multiple records from the Aerospike DB in parallel
            as_results = client.get_many(as_keys)

            # The remainder is all formatting for the return from the HTTP request
            res = {"target": aTicker}
            res["datapoints"] = []
        
            for aRes in as_results:
                if aRes[1] is None:
                    continue 
                dps = aRes[2]
                for bin in dps["datapoints"]:
                    tmp_bin = bin[0]
                    bin[0] = float(bin[1])
                    bin[1] = tmp_bin
                    res["datapoints"].append(bin)
            results.append(res)

    #Return the results to the caller
    return jsonify(results)

if __name__ == '__main__':

    # Start listening on port 3334. Change as desired.
    app.run(host='0.0.0.0', port=3334, debug=True)
