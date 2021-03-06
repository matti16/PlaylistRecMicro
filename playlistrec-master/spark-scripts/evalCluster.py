# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

import time, json
import re
from os import path


def computeMetrics(conf, recRDD):
    splitPath = path.join(conf['general']['bucketName'], conf['general']['clientname'])
    # basePath = "s3n://" + conf['general']['bucketName'] + "/"+conf['general']['clientname']+"/"
    GTpath = path.join(splitPath, "GT")
    # GTpath = splitPath+"GT"

    algo_conf = conf['algo']['name'] + '_' + \
                '#'.join([str(v) for k, v in conf['algo']['props'].iteritems()])
    algo_conf = re.sub(r'[^A-Za-z0-9#_]', '', algo_conf)

    confPath = path.join(splitPath, 'Rec', algo_conf)
    recPath = path.join(confPath, "recommendations")
    # recPath = splitPath+"/Rec/"+ conf['algo']['name']+"/recommendations/"

    gtRDD = sc.textFile(GTpath).map(lambda x: json.loads(x))
    recRDD = recRDD.map(lambda x: json.loads(x))

    recommendationRDD = recRDD \
        .flatMap(lambda x: ([(x['id'], (k['id'], k['rank'], x)) for k in x['linkedinfo']['response']]))

    groundTruthRDD = gtRDD \
        .flatMap(lambda x: ([(x['linkedinfo']['gt'][0]['id'], (k['id'], x)) for k in x['linkedinfo']['objects']]))

    
    hitRDDPart = recommendationRDD.join(groundTruthRDD).filter(lambda x: x[1][0][0] == x[1][1][0])
    
    #hitRDDPart.map(lambda x: {"type": "linebyline", "id": -1, "ts": -1, "linkedinfo":
    #    {"recom": x[1][0][2], "GT": x[1][1][1]}}).map(lambda x: json.dumps(x)) \
    #    .repartition(10) \
    #    .saveAsTextFile(path.join(confPath, conf['evaluation']['name'], "lineByLine"))
    # .saveAsTextFile(path.join(confPath, "lineByLine"))

    hitRDD = hitRDDPart.map(lambda x: (x[0], x[1][0][1], 1.0))
    '''
    {"type": "metric", "id": -1, "ts" : -1, "properties": {"name": "recall@20" ,"value": 0.25}, 
    "linkedinfo":{"subjects":[], "objects" : [] }}
    '''
    totRec = float(groundTruthRDD.count())
    result = []

    for n in conf['evaluation']['metric']['prop']['N']:
        temp = {}
        temp['type'] = 'metric'
        temp['id'] = -1
        temp['ts'] = time.time()
        temp['properties'] = {}
        temp['properties']['name'] = conf['evaluation']['name']
        temp['evaluation'] = {}
        temp['evaluation']['N'] = n
        temp['evaluation']['value'] = hitRDD.filter(lambda x: x[1] < n).map(lambda x: x[2]).sum() / totRec
        temp['linkedinfo'] = {}
        temp['linkedinfo']['subjects'] = []
        temp['linkedinfo']['subjects'].append({})
        temp['linkedinfo']['subjects'][0]['splitName'] = conf['split']['name']
        temp['linkedinfo']['subjects'][0]['algoName'] = conf['algo']['name']
        result.append(temp)
    metricsPath = path.join(confPath, conf['evaluation']['name'], "metrics")
    (sc
     .parallelize(result)
     .map(lambda x: json.dumps(x))
     .repartition(1)
     .saveAsTextFile(metricsPath))
    print "%s successfully written to %s" % (conf['evaluation']['name'], metricsPath)




def computeMetrics_precision(conf, recRDD):
    splitPath = path.join(conf['general']['bucketName'], conf['general']['clientname'])
    # basePath = "s3n://" + conf['general']['bucketName'] + "/"+conf['general']['clientname']+"/"
    GTpath = path.join(splitPath, "GT")
    # GTpath = splitPath+"GT"

    algo_conf = conf['algo']['name'] + '_' + \
                '#'.join([str(v) for k, v in conf['algo']['props'].iteritems()])
    algo_conf = re.sub(r'[^A-Za-z0-9#_]', '', algo_conf)

    confPath = path.join(splitPath, 'Rec', algo_conf)
    recPath = path.join(confPath, "recommendations")
    # recPath = splitPath+"/Rec/"+ conf['algo']['name']+"/recommendations/"

    gtRDD = sc.textFile(GTpath).map(lambda x: json.loads(x))
    recRDD = recRDD.map(lambda x: json.loads(x))
    n_rec = recRDD.count()

    recommendationRDD = recRDD \
        .flatMap(lambda x: ([(x['id'], (k['id'], k['rank'], x)) for k in x['linkedinfo']['response']]))

    groundTruthRDD = gtRDD \
        .flatMap(lambda x: ([(x['linkedinfo']['gt'][0]['id'], (k['id'], x)) for k in x['linkedinfo']['objects']]))

    
    '''IMPLEMENT PRECISION'''

    hitRDDPart = recommendationRDD.join(groundTruthRDD).filter(lambda x: x[1][0][0] == x[1][1][0])
    

    hitRDD = hitRDDPart.map(lambda x: (x[0], x[1][0][1], 1.0))
    '''
    {"type": "metric", "id": -1, "ts" : -1, "properties": {"name": "precision@20" ,"value": 0.25}, 
    "linkedinfo":{"subjects":[], "objects" : [] }}
    '''
    result = []

    for n in conf['evaluation']['metric']['prop']['N']:
        temp = {}
        temp['type'] = 'metric'
        temp['id'] = -1
        temp['ts'] = time.time()
        temp['properties'] = {}
        temp['properties']['name'] = conf['evaluation']['name']
        temp['evaluation'] = {}
        temp['evaluation']['N'] = n
        temp['evaluation']['value'] = hitRDD.filter(lambda x: x[1] < n).map(lambda x: x[2]).sum() / (n*n_rec)
        temp['linkedinfo'] = {}
        temp['linkedinfo']['subjects'] = []
        temp['linkedinfo']['subjects'].append({})
        temp['linkedinfo']['subjects'][0]['splitName'] = conf['split']['name']
        temp['linkedinfo']['subjects'][0]['algoName'] = conf['algo']['name']
        result.append(temp)
    metricsPath = path.join(confPath, conf['evaluation']['name'], "metrics")
    (sc
     .parallelize(result)
     .map(lambda x: json.dumps(x))
     .repartition(1)
     .saveAsTextFile(metricsPath))
    print "%s successfully written to %s" % (conf['evaluation']['name'], metricsPath)