#!/usr/bin/env python

import argparse
import urllib
import json

def parseArgs():
    myParser = argparse.ArgumentParser(description="Kimono Labs API Crawled Data Retriever. This script will retrieve all JSON results from an API (latest version of the data only) and send to STDOUT.")
    myParser.add_argument("--api-key", required=True, help="Your kimonolabs API key")
    myParser.add_argument("api_id", help="This is the API ID that you want to retrieve results from")
    myParser.add_argument("--version", help="This is the version number for the data to receive (omit to receive the latest)", default=None, type=int)
    myParser.add_argument("--timeseries", action="store_true")
    grp = myParser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--json", action="store_true", help="Retrieves results as JSON")
    grp.add_argument("--csv", action="store_true", help="Retrieves results as CSV")
    return myParser.parse_args()

def main():
    args = parseArgs()
    baseURL = "https://www.kimonolabs.com/api/"
    if args.csv:
        baseURL = baseURL + "csv/"
    if args.version is not None:
        baseURL = baseURL + str(args.version) + "/"
    baseURL = baseURL + args.api_id + "?apikey=" + args.api_key
    if args.timeseries:
        baseURL = baseURL + "&kimseries=1"
    
    linesRead = 0
    limit = 2500
    offset = 0
    stayInLoop = True
    extras = ""
    readJSON = None
    while stayInLoop:
        f = urllib.urlopen(baseURL + "&kimoffset=" + str(offset) + "&kimlimit=" + str(limit) + extras)
        if args.json:
            readBuffer = f.read()
            if readJSON is None:
                readJSON = json.loads(readBuffer)
                linesRead = readJSON["count"]
                if linesRead < limit:
                    stayInLoop = False
            else:
                temp = json.loads(readBuffer)
                readJSON["results"]["collection1"].extend(temp["results"]["collection1"])
                linesRead = linesRead + temp["count"]
                if temp["count"] < limit:
                    stayInLoop = False
            if not stayInLoop:
                readJSON["count"] = linesRead
        elif args.csv:
            readBuffer = f.readlines()
            numLines = len(readBuffer) - 4 #we've got two header lines and two footer lines that inflate the record count
            for line in readBuffer:
                if (not '"Pagination"' in line) or (len(line) <= 1):
                    print line,
            if numLines < limit:
                # we're done querying this data source - no more results
                stayInLoop = False
            extras = "&kimnoheaders=1"

        offset +=  limit

    if args.json:
        print json.dumps(readJSON)

if __name__ == "__main__":
    main()