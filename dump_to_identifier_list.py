import sys
import os
import codecs
import json


def main(argv=None):
    
    linedump_filename = "collections_dump.json"
    
    with codecs.open( linedump_filename, encoding='utf-8', mode='r' ) as read_handle:

        for rln in read_handle:
            ln = rln.strip()
            try:
                if ln[0] == ",":
                    res = json.loads( ln[1:] )
                elif ln[0] == "{":
                    res = json.loads( ln )
                else:
                    badline
            except:
                print "Skipping %s" % ln
                continue            

            print res["_id"]


if __name__ == "__main__":
    sys.exit(main())
