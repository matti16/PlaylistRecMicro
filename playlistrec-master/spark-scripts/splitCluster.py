# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

import numpy as np
import json
import os
from pyspark import StorageLevel
#from conventions import *
#from utils import *

def testTrainUserSplit(x, percUsTr):
    if percUsTr * 100 <= np.random.randint(0, 100): return (x, 1)
    return (x, 0)


def creaOnlineTraining(x, noRepet, prop, GT=0, tr=0):
    result = list()
    REQ = {}
    REQ['type'] = 'request'
    REQ['id'] = createId(x)
    REQ['ts'] = x['ts']
    REQ['properties'] = prop
    REQ['linkedinfo'] = {}
    REQ['linkedinfo']['objects'] = []

    alreadyAdded = set()
    conta = 0
    for k in x['linkedinfo']['objects']:
        if len(REQ['linkedinfo']['objects']) >= tr: break
        conta += 1
        if k['id'] in alreadyAdded and noRepet: continue
        alreadyAdded.add(k['id'])
        REQ['linkedinfo']['objects'].append(k)
    REQ['linkedinfo']['subjects'] = x['linkedinfo']['subjects']
    result.append(REQ)
    ## GT
    GT = {}
    GT['type'] = x['type']
    GT['id'] = x['id']
    GT['ts'] = x['ts']
    GT['linkedinfo'] = {}
    GT['linkedinfo']['subjects'] = x['linkedinfo']['subjects']
    GT['linkedinfo']['objects'] = []
    GT['linkedinfo']['gt'] = []
    GT['linkedinfo']['gt'].append({})
    GT['linkedinfo']['gt'][0]['type'] = 'request'
    GT['linkedinfo']['gt'][0]['id'] = REQ['id']

    ## QUA mi dovrebbe ritornare un elemento per ciascun evento della sessione
    if conta > len(x['linkedinfo']['objects']): return result
    for k in x['linkedinfo']['objects'][conta:]:
        if len(GT['linkedinfo']['objects']) >= tr: break
        conta += 1
        if k['id'] in alreadyAdded and noRepet: continue
        alreadyAdded.add(k['id'])
        GT['linkedinfo']['objects'].append(k)
    result.append(GT)
    return result


def createId(user, ts=0):
    if type(user) == dict and long(ts) == 0:
        return long(user['linkedinfo']['subjects'][0]['id']) * 100000000000 + long(user['ts'])
    elif type(user) == dict and long(ts) != 0:
        return long(user['linkedinfo']['subjects'][0]['id']) * 100000000000 + long(ts)
    else:
        return user * 100000000000 + ts


def gt1Creator(x, prop, mode='ts-1', TS=0):
    ## REQ
    if mode == 'ts-1' or mode == 'req':
        if mode == 'req':
            x['ts'] = TS
        ident = createId(x)
        REQ = {}
        REQ['type'] = 'request'
        REQ['id'] = createId(x)
        REQ['ts'] = x['ts']
        REQ['properties'] = prop
        REQ['linkedinfo'] = {}
        REQ['linkedinfo']['objects'] = []
        REQ['linkedinfo']['subjects'] = x['linkedinfo']['subjects']
    if mode == 'req': return REQ
    ## GT
    if mode == 'ts-multi':
        ident = createId(x, TS)
    GT = {}
    GT['type'] = x['type']
    GT['id'] = x['id']
    GT['ts'] = x['ts']
    GT['linkedinfo'] = {}
    GT['linkedinfo']['objects'] = [x['linkedinfo']['objects'][0]]
    GT['linkedinfo']['subjects'] = x['linkedinfo']['subjects']
    GT['linkedinfo']['gt'] = []
    GT['linkedinfo']['gt'].append({})
    GT['linkedinfo']['gt'][0]['type'] = 'request'
    GT['linkedinfo']['gt'][0]['id'] = ident

    if mode == 'ts-multi': return GT
    return (GT, REQ)


def remove_zz_top(x):
    ZZ_TOP = 3893303

    objects = x['linkedinfo']['objects']
    x['linkedinfo']['objects'] = []

    for i in objects:
        if i['id'] < ZZ_TOP:
            x['linkedinfo']['objects'].append(i)
    
    return x



def splitter(conf):
    prop = conf[SPLIT][PROP]
    pathOUT = conf[SPLIT][OUT]
    # check for already existing splits

    minEventsPerUser = conf['split']['minEventsPerUser'] if 'minEventsPerUser' in conf['split'] else 0
    mode = conf[SPLIT][MODE]
    TS = conf[SPLIT][TS_json]

    if conf[SPLIT]['type'] == list:
        RDD = sc.parallelize([])
        for key_list in mybucket.list():
            if conf[SPLIT][LOCATION] + "/" in key_list.name and '_SUCCESS' in key_list.name:
                path = 's3n://' + conf[BUCKET] + "/" + key_list.name.replace('_SUCCESS', '')
                RDD = RDD.union(sc.textFile(path))

    else:
        RDD = sc.textFile(conf[GENERAL][BUCKET] + "/" + conf[SPLIT][LOCATION])


    readDataset = RDD.map(lambda x: json.loads(x)).map(remove_zz_top)
    readDataset = readDataset.map(lambda x: (x[LINKEDINFO][SUBJECTS][0][ID], x)).persist(StorageLevel.MEMORY_AND_DISK)

    lowActUsersRDD = readDataset.map(lambda x: (x[0], 1)) \
        .reduceByKey(lambda x, y: x + y).filter(lambda x: x[1] <= minEventsPerUser).persist()

    readDataset2 = readDataset.subtractByKey(lowActUsersRDD).persist(StorageLevel.MEMORY_AND_DISK)

    readDataset.unpersist()

    if mode == 'session' or 'total':
        percUsTr = conf['split']['percUsTr']
        minEventPerSession = conf['split']['minEventPerSession']
        onlineTrainingLength = conf['split']['onlineTrainingLength']
        GTlength = conf['split']['GTlength']
        minEventPerSessionTraining = conf['split']['minEventPerSessionTraining']
        minEventPerSessionTest = conf['split']['minEventPerSessionTest']

        splitTestTrain = readDataset2.map(lambda x: x[0]).distinct().map(lambda x: testTrainUserSplit(x, percUsTr)) \
            .persist(StorageLevel.MEMORY_AND_DISK)

        testUsersRDD = splitTestTrain.filter(lambda x: x[1] == 1)
        trainUsersRDD = splitTestTrain.filter(lambda x: x[1] == 0)

        readDataset2.filter(lambda x: len(x[1]['linkedinfo']['objects']) >= minEventPerSessionTraining) \
            .join(trainUsersRDD).map(lambda x: json.dumps(x[1][0])) \
            .saveAsTextFile(os.path.join(pathOUT, "train/batchTraining/"))
        testRDD = readDataset2.filter(lambda x: len(x[1]['linkedinfo']['objects']) >= minEventPerSessionTest) \
            .join(testUsersRDD).map(lambda x: (long(x[1][0]['ts']), x[1][0]))

        testRDD.filter(lambda x: long(x[0]) <= TS).map(lambda x: json.dumps(x[1])) \
            .saveAsTextFile(os.path.join(pathOUT, "test/batchTraining/"))

        if mode == 'session':
            recAndGt = testRDD.filter(lambda x: long(x[0]) > TS) \
                .flatMap(
                lambda x: creaOnlineTraining(x[1], conf['split']['excludeAlreadyListenedTest'], prop, GT=GTlength,
                                             tr=onlineTrainingLength))
        else:
            recAndGt = testRDD.filter(lambda x: long(x[0]) > TS) \
                .flatMap(lambda x: creaOnlineTraining(x[1], conf['split']['excludeAlreadyListenedTest'], prop,
                                                      GT=1000000, tr=onlineTrainingLength))

        recAndGt.filter(lambda x: x['type'] == 'request').map(lambda x: json.dumps(x)) \
            .saveAsTextFile(os.path.join(pathOUT, "test/onlineTraining/"))
        recAndGt.filter(lambda x: x['type'] != 'request').map(lambda x: json.dumps(x)) \
            .saveAsTextFile(os.path.join(pathOUT, "GT/"))

    if mode == 'ts-1' or mode == 'ts-multi':
        splitTestTrain = readDataset2.map(lambda x: (x[1]['ts'], x[1])).persist()
        splitTestTrain.filter(lambda x: long(x[0]) <= TS).map(lambda x: json.dumps(x[1])) \
            .saveAsTextFile(pathOUT + "/train/")
        test = splitTestTrain.filter(lambda x: long(x[0]) > TS)
        if mode == 'ts-1':
            recAndGt = test.flatMap(lambda x: gt1Creator(x[1], prop))
        else:
            recAndGt = test.map(lambda x: gt1Creator(x[1], prop, mode='ts-multi', TS=TS)) \
                .union(test.map(lambda x: (x[1]['linkedinfo']['subjects'][0]['id'], x)) \
                       .reduceByKey(lambda x, y: x).map(lambda x: gt1Creator(x[1][1], prop, mode='req', TS=TS)))
        recAndGt.filter(lambda x: x['type'] == 'request').map(lambda x: json.dumps(x)).repartition(16) \
            .saveAsTextFile(os.path.join(pathOUT, "test/request/"))
        recAndGt.filter(lambda x: x['type'] != 'request').map(lambda x: json.dumps(x)).repartition(16) \
            .saveAsTextFile(os.path.join(pathOUT, "GT/"))
