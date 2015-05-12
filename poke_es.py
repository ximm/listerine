# -*- coding: utf-8 -*-
# Copyright(c)2005-(c)2015 Internet Archive. Software license GPL version 2.

# Elasticsearch toolbox for the Internet Archive
#  Mike McCabe
#  Aaron Ximm

import sys
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json

import csv
import unicodedata # ?

import random

import logging
logging.basicConfig()

es = None

def jp_print( json_str ):
    print json.dumps( json_str, indent=2, separators=(',', ': ') )

def healthcheck(clusterhealth):
    if clusterhealth:
        res = es.cluster.health()
    else:
        res = es.nodes.info(node_id='_local',human=True)
    jp_print( res )
    
def sq(frum=0, size=10):
    body = {
    "size": size,
    "from": frum,
    "fields": ["_id"],
    "query" : {
            "term" : { "languageSorter": "eng" }
            }
    # "query" : {
    #         "term" : { "indexflag": "excluded" }
    #         }
    }
    # res = es.get(index="wcd", id="wcd_the-winter-ep_cusp_flac_lossless_30832239")
    res = es.search(index='archive-all', doc_type='item', body=body)
    return res


def scan():
    # body = {
    # "size": 1200000,
    # "from": 0,
    # "fields": ["_id"],
    # }
    # query='{ "query": { "term" : { "indexflag": "excluded" } } }'
    # for lang in ('en-us', 'kor', 'korean', 'english', 'spa', 'spanish', 'Espanol', 'arabic', 'ind', 'ara', 'bosanski', 'urd', 'jpn', 'fre', 'indonesian', 'ger', 'ita', 'italian', 'danish', 'hin', 'swe', 'latin', 'rum', 'czech', 'malay', 'eng', 'bosanski', 'por', 'rus', 'tha', '1433', 'English-handwritten', 'german', 'panjabi', 'Panjabi', 'portuguese', 'tur', 'Www.rabania.com', 'English/French', 'English; Finnish', 'Finnish, English', 'En-ca', 'Zun', 'Zh', 'No lingustic content', 'No', 'undetermined', 'Unknown', 'None', 'No speech', 'No language', 'Qaa', '|d'):

    for lang in ('None', 'Qaa'):

        query='{ "query": { "term" : { "languageSorter":"' + lang + '"} } }'

# , 'हिंदी'
    # res = es.get(index="wcd", id="wcd_the-winter-ep_cusp_flac_lossless_30832239")
        res = helpers.scan(es, index='archive-all', query=query, scroll='20m', fields='_id')
        for hit in res:
            jp_print(hit)
    return res


def query(frum, size):
    body = {
    "size" : size,
    "from" : frum,
    "fields": ["_id", "parent"],
    "query" : {
            "term" : { "external-identifier": "urn:acoustid:unknown" }
            }
    }
    res = es.search(index="wcd", body=body)
    json.dumps(res, indent=4)
    return res

def query_all():
    hits = []
    
    frum = 0
    size = 100

    last = False
    while True:
        if len(hits):
            yield hits.pop()
            continue
        elif last:
            break
        else:
            # res = query(frum, size)
            res = sq(frum, size)
            hits = res['hits']['hits']
            frum += len(hits)
            if len(hits) < size:
                last = True

def lq():
    for hit in query_all():
        # print hit['fields']['parent'][0]
        print hit['_id']


# random.seed(1)
# sample = res['hits']['hits']
# #comment previous line, and un-comment next line for a random sample instead
# #randomsample = random.sample(res['hits']['hits'], 5);  #change int to RANDOMLY SAMPLE a certain number of rows from your query  

# print("Got %d Hits:" % res['hits']['total'])

# with open('outputfile.tsv', 'wb') as csvfile:   #set name of output file here
# 	filewriter = csv.writer(csvfile, delimiter='\t',  # we use TAB delimited, to handle cases where freeform text may have a comma
#                             quotechar='|', quoting=csv.QUOTE_MINIMAL)
# 	# create header row
# 	filewriter.writerow(["id", "column2", "column3"])    #change the column labels here
# 	for hit in sample:   #switch sample to randomsample if you want a random subset, instead of all rows
# 		try:			 #try catch used to handle unstructured data, in cases where a field may not exist for a given hit
# 			col1 = hit["_id"]
# 		except Exception, e:
# 			col1 = ""
# 		try:
# 			col2 = hit["some"]["deeply"]["nested"]["field"].decode('utf-8')  #replace these nested key names with your own
# 			col2 = col2.replace('\n', ' ')
# 		except Exception, e:
# 			col2 = ""
# 		try:
# 			col3 = hit["someother"]["deeply"]["nested"]["field"].decode('utf-8')  #replace these nested key names with your own
# 			col3 = col3.replace('\n', ' ')
# 		except Exception, e:
# 			col3 = ""
# 		filewriter.writerow([col1,col2,col3])




# ADD ANALYZER TO INDEX AFTER CREATED
# curl -XPOST 'localhost:9200/myindex/_close'

# curl -XPUT 'localhost:9200/myindex/_settings' -d '{
#   "analysis" : {
#     "analyzer":{
#       "content":{
#         "type":"custom",
#         "tokenizer":"whitespace"
#       }
#     }
#   }
# }'

# curl -XPOST 'localhost:9200/myindex/_open'




def create_index():
    # es.indices.delete(index=args.index, ignore=404)
    es.indices.create(index=args.index, body={
            "settings" : {
                "index" : {
                    "number_of_shards" : 5,
                    "refresh_interval": "5s",
                    "number_of_replicas" : 2,
                    "analysis": {
                        "analyzer" : {
                            "textBar": {
                                "type": "custom",
                                "tokenizer": "whitespace",
                                "filter": ["lowercase", "asciifolding"]                                }
                            }
                        },
                    },
                "merge": { "policy": { "use_compound_file": False } }
                },

            "mappings" : {
                "item" : {
                    "_source" : {"excludes" : ["uploader", "text"]},
                    "properties": {
                        "mediatype": { "type": "string", "index": "analyzed", "analyzer": "textBar" },
                        "text": { "type": "string", "index": "analyzed", "analyzer": "textBar" },
                        "type": { "type": "string", "index": "analyzed", "analyzer": "textBar" },
                        "collection": { "type": "string", "index": "analyzed", "analyzer": "textBar" },
                        "members": { "type": "string", "index": "analyzed", "analyzer": "textBar" },
                        
                        # these are extended types for cached collection graph support and enumerated lists
                        # since they are subject to term[s] filters they must be stored unanalyzed
                        "members_original": { "type": "string", "index" : "not_analyzed" },
                        "collection_reduced": { "type": "string", "index": "not_analyzed" },
                        "collection_expanded": { "type": "string", "index": "not_analyzed" },
                        "sub_collection_expanded": { "type": "string", "index": "not_analyzed" },
                        "list_reduced": { "type": "string", "index": "not_analyzed" },
                        "list_expanded": { "type": "string", "index": "not_analyzed" },
                        "sub_list_expanded": { "type": "string", "index": "not_analyzed" },
                        "set_reduced": { "type": "string", "index": "not_analyzed" },
                        "set_expanded": { "type": "string", "index": "not_analyzed" },
                        "sub_set_expanded": { "type": "string", "index": "not_analyzed" },

                        "format": { "type": "string", "index": "analyzed", "analyzer": "textBar" },
                        "identifier": { "type": "string", "index": "analyzed", "analyzer": "textBar" },

                        # these are for facets -- dont whitespace split and lowercase values -- treat AS IS!
                        "titleSorter":          {"type" : "string", "index" : "not_analyzed"},
                        "creatorSorter":        {"type" : "string", "index" : "not_analyzed"},
                        "languageSorter":       {"type" : "string", "index" : "not_analyzed"},
                        "subjectSorter":        {"type" : "string", "index" : "not_analyzed"},
                        "forumSubjectSorter":   {"type" : "string", "index" : "not_analyzed"},
                        "forumPosterSorter":    {"type" : "string", "index" : "not_analyzed"},
                        "licenseurl":    {"type" : "string", "index" : "not_analyzed"},
                        "headerImage":    {"type" : "string", "index" : "not_analyzed"},
                        "call_number":    {"type" : "string", "index" : "not_analyzed"},

                        "avg_rating": {"type": "float"},
                        "downloads": {"type": "long"},
                        "week": {"type": "integer"},
                        "year": {"type": "integer"},
                        "stars": {"type": "integer"},
                        "nav_order": {"type": "integer"},
                        "num_reviews": {"type": "integer"},
                        "imagecount": {"type": "integer"},
                        "forumId": {"type": "integer"},
                        "addeddate": {"type" : "date"},
                        "createdate": {"type" : "date"},
                        "date": {"type" : "date"},
                        "forumPostdate": {"type" : "date"},
                        "indexdate": {"type" : "date"},
                        "oai_updatedate": {"type" : "date"},
                        "publicdate": {"type" : "date"},
                        "reviewdate": {"type" : "date"},
                        "updatedate": {"type" : "date"},
                        }
                    }
                }
            })
    jp_print( es.indices.get_settings(index=args.index, flat_settings=False))


def delete_index():
    jp_print( es.indices.delete(index=args.index, ignore=404) )


def put_index_template():
    body = {
        "template" : "web-*",
        "settings" : {
            # "index.analysis.analyzer.default.type": "simple",
            # "index.cache.field.type": "soft",
            # "index.compress.stored": true,
            # "index.merge.policy.max_merged_segment": "5g",
            # "index.query.default_field": "@message",
            "index.refresh_interval": "5s",
            "index.routing.allocation.include.datacenter": "richmond",
            # "index.term_index_divisor": 1,
            # "index.term_index_interval": 128,
            "number_of_shards": 15,
            "number_of_replicas": 0
            },
        "mappings": {
            "_default_": {
                "_all": {
                    "enabled": False
                    },
                "properties": {
                    "@message": {
                        "type": "string",
                        "index": "analyzed"
                        },
                    "@source": {
                        "type": "string",
                        "index": "not_analyzed"
                        },
                    "@source_host": {
                        "type": "string",
                        "index": "not_analyzed"
                        },
                    "@source_path": {
                        "type": "string",
                        "index": "not_analyzed"
                        },
                    "@tags": {
                        "type": "string",
                        "index": "not_analyzed"
                        },
                    "@timestamp": {
                        "type": "date",
                        "index": "not_analyzed"
                        },
                    "@type": {
                        "type": "string",
                        "index": "not_analyzed"
                        }
                    }
                }
            }
        }

    # print es.indices.delete_template(name='web_template')
    jp_print( es.indices.put_template(name='web_template',
                                  body=body, create=True))


def get_index_template():
    pass


def put_settings():
    body = {
        "index" : {
            # "refresh_interval" : "-1",
            # "merge.policy.merge_factor": 30,
            "number_of_replicas": 2
            } }

    # body = { "subitem" : {
    #         "_parent" : {
    #             "type" : "item"
    #             }
    #         } }

    # body= { "settings":
    #             { "index" :
    #                   { "refresh_interval" : "60s",
    #                     "merge.policy.merge_factor": 30 } } }

    # default merge_factor is 10?
    r = es.indices.put_settings(index=args.index, body=body)
    jp_print( r )
                                       


def get_settings():
    jp_print( es.indices.get_settings(index=args.index, flat_settings=True) )


def closef():
    jp_print( es.indices.close(index=args.index) )


def openf():
    jp_print( es.indices.open(index=args.index) )


def optimize():
    jp_print( es.indices.optimize(index=args.index) )


def get():
    jp_print( es.get(index=args.index, id=args.get) )


def main():
    # XXX should be optparse
    import argparse
    
    global es
    
    parser = argparse.ArgumentParser()
    parser.add_argument('id', nargs='?', default=False)
    parser.add_argument('--create',
                        help='wipe/create index',
                        action='store_true')
    parser.add_argument('--delete',
                        help='delete an index',
                        action='store_true')
    parser.add_argument('--host', 
                        help='host queried',    
                        action='store')
    parser.add_argument('--get_settings', action='store_true')
    parser.add_argument('--put_settings', action='store_true')
    parser.add_argument('--put_doc', action='store_true')
    parser.add_argument('--get')
    parser.add_argument('--index', default='none')
    parser.add_argument('--close', action='store_true')
    parser.add_argument('--open', action='store_true')
    parser.add_argument('--optimize', action='store_true')
    parser.add_argument('--put_index_template', action='store_true')
    parser.add_argument('--query', action='store_true')
    parser.add_argument('--lq', action='store_true')
    parser.add_argument('--sq', action='store_true')
    parser.add_argument('--scan', action='store_true')
    parser.add_argument('--healthcheck',
                        help='for node if host specified, else cluster',
                        action='store_true')
    global args
    args = parser.parse_args()

    if args.host:
        es = Elasticsearch( args.host )
    else:
#        if args.healthcheck:
#            parser.error('--healthcheck requires --host HOST')    
        es = Elasticsearch( 'es-lb' )

    if args.create:
        create_index()
    elif args.delete:
        delete_index()
    elif args.get_settings:
        get_settings()
    elif args.put_settings:
        put_settings()
    elif args.close:
        closef()
    elif args.open:
        openf()
    elif args.optimize:
        optimize()
    elif args.put_index_template:
        put_index_template()
    elif args.query:
        query()
    elif args.lq:
        lq()
    elif args.sq:
        sq()
    elif args.get:
        get()
    elif args.scan:
        scan()
    elif args.healthcheck:
        healthcheck( args.host==None )

    else:
        parser.print_help()
        sys.exit(0)
    
    return 0

if __name__ == '__main__':
    sys.exit(main())
