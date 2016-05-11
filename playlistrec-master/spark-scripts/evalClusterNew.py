# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

import time, json
import re
from os import path


def map_hits(x, n):
    try:
        result = 0
        ranks_used = set()
        id_used = set()
        gt = x[1][1]

        for rec in x[1][0]:
            rec_id = rec[0]
            rec_rank = rec[1]

            if rec_rank >= n:
                continue

            if rec_rank in ranks_used:
                continue
            if rec_id in id_used:
                continue
            
            if rec_id in gt:
                ranks_used.add(rec_rank)
                id_used.add(rec_id)
                result += 1

    except Exception:
        raise Exception(str(x))
    return result


def map_hits_with_loss(x, n):
    try:
        result = 0
        ranks_used = set()
        id_used = set()
        for rec in x[1][0]:
            if rec[0] in x[1][1] and rec[1] < n:
                if not rec[1] in ranks_used and not rec[0] in id_used:
                    ranks_used.add(rec[1])
                    id_used.add(rec[0])
                    result += 1
                elif rec[1] in ranks_used:
                    result -= 1
    except Exception:
        raise Exception(str(x))
    return result


def map_loss(x,n):
    try:
        result = 0
        ranks_used = set()
        for rec in x[1][0]:
            if rec[0] in x[1][1] and rec[1] < n:
                if not rec[1] in ranks_used:
                    ranks_used.add(rec[1])
                else:
                    result += 1
    except Exception:
        raise Exception(str(x))
    return result


def computeNewRecallPrecision(conf, recRDD, loss = False, plain = False, path):
    DATA_PATH = '/home/jovyan/work/data/mattia/results'

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
    n_rec = float(gtRDD.count())

    recRDD = recRDD.map(lambda x: (x['id'], [(i['id'], i['rank']) for i in x['linkedinfo']['response']]))

    groundTruthRDD = gtRDD \
        .flatMap(lambda x: ([(x['linkedinfo']['gt'][0]['id'], (k['id'], x)) for k in x['linkedinfo']['objects']]))

    gtRDD = gtRDD.map(lambda x: (x['linkedinfo']['gt'][0]['id'], [i['id'] for i in x['linkedinfo']['objects']]))

    hitRDD = recRDD.join(gtRDD)

    '''
    {"type": "metric", "id": -1, "ts" : -1, "properties": {"name": "recall@20" ,"value": 0.25}, 
    "linkedinfo":{"subjects":[], "objects" : [] }}
    '''

    totRec = float(groundTruthRDD.count())
    result = []

    if not plain:
        conf['evaluation']['name'] = 'newRecall@N' if not loss else 'newLossRecall@N'
    else:
        conf['evaluation']['name'] = 'plain/newRecall@N' if not loss else 'plain/newLossRecall@N'


    values = {}
    for n in conf['evaluation']['metric']['prop']['N']:
        if not loss:
            values[n] = hitRDD.map(lambda x: map_hits(x, n)).sum()
        else:
            values[n] = hitRDD.map(lambda x: map_hits_with_loss(x, n)).sum()


    for n in conf['evaluation']['metric']['prop']['N']:
        temp = {}
        temp['type'] = 'metric'
        temp['id'] = -1
        temp['ts'] = time.time()
        temp['properties'] = {}
        temp['properties']['name'] = conf['evaluation']['name']
        temp['evaluation'] = {}
        temp['evaluation']['N'] = n
        temp['evaluation']['value'] = float(values[n]) / totRec
        temp['linkedinfo'] = {}
        temp['linkedinfo']['subjects'] = []
        temp['linkedinfo']['subjects'].append({})
        temp['linkedinfo']['subjects'][0]['splitName'] = conf['split']['name']
        temp['linkedinfo']['subjects'][0]['algoName'] = conf['algo']['name']
        result.append(temp)


    with open(path.join(DATA_PATH, path, 'recall@N'), 'w') as f:
        for i in result:
            line = json.dumps(i)
            f.write(i + '\n')

    '''
    metricsPath = path.join(confPath, conf['evaluation']['name'], "metrics")
    (sc
     .parallelize(result)
     .map(lambda x: json.dumps(x))
     .repartition(1)
     .saveAsTextFile(metricsPath))
     '''
    print "%s successfully written to %s" % (conf['evaluation']['name'], metricsPath)



    '''COMPUTE PRECISION'''
    result = []

    if not plain:
        conf['evaluation']['name'] = 'newPrecision@N' if not loss else 'newLossPrecision@N'
    else:
        conf['evaluation']['name'] = 'plain/newPrecision@N' if not loss else 'plain/newLossPrecision@N'

    for n in conf['evaluation']['metric']['prop']['N']:
        temp = {}
        temp['type'] = 'metric'
        temp['id'] = -1
        temp['ts'] = time.time()
        temp['properties'] = {}
        temp['properties']['name'] = conf['evaluation']['name']
        temp['evaluation'] = {}
        temp['evaluation']['N'] = n
        temp['evaluation']['value'] = float(values[n]) / (n*n_rec)
        temp['linkedinfo'] = {}
        temp['linkedinfo']['subjects'] = []
        temp['linkedinfo']['subjects'].append({})
        temp['linkedinfo']['subjects'][0]['splitName'] = conf['split']['name']
        temp['linkedinfo']['subjects'][0]['algoName'] = conf['algo']['name']
        result.append(temp)


    with open(path.join(DATA_PATH, path, 'precision@N'), 'w') as f:
        for i in result:
            line = json.dumps(i)
            f.write(i + '\n')

    '''
    metricsPath = path.join(confPath, conf['evaluation']['name'], "metrics")
    (sc
     .parallelize(result)
     .map(lambda x: json.dumps(x))
     .repartition(1)
     .saveAsTextFile(metricsPath))
     '''
    print "%s successfully written to %s" % (conf['evaluation']['name'], metricsPath)











def computeClusterLoss(conf, recRDD):
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
    n_rec = float(recRDD.count())

    recRDD = recRDD.map(lambda x: (x['id'], [(i['id'], i['rank']) for i in x['linkedinfo']['response']]))

    groundTruthRDD = gtRDD \
        .flatMap(lambda x: ([(x['linkedinfo']['gt'][0]['id'], (k['id'], x)) for k in x['linkedinfo']['objects']]))

    gtRDD = gtRDD.map(lambda x: (x['linkedinfo']['gt'][0]['id'], [i['id'] for i in x['linkedinfo']['objects']]))

    hitRDD = recRDD.join(gtRDD)

    '''
    {"type": "metric", "id": -1, "ts" : -1, "properties": {"name": "recall@20" ,"value": 0.25}, 
    "linkedinfo":{"subjects":[], "objects" : [] }}
    '''

    totRec = float(groundTruthRDD.count())
    result = []

    conf['evaluation']['name'] = 'clusterLoss@N'

    for n in conf['evaluation']['metric']['prop']['N']:
        temp = {}
        temp['type'] = 'metric'
        temp['id'] = -1
        temp['ts'] = time.time()
        temp['properties'] = {}
        temp['properties']['name'] = conf['evaluation']['name']
        temp['evaluation'] = {}
        temp['evaluation']['N'] = n
        temp['evaluation']['value'] = hitRDD.map(lambda x: map_loss(x, n)).sum() / totRec
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
