__author__ = 'robertopagano'

import json
import re
from os import path
from operator import itemgetter

'''
Matrix multiplication, similarity and kNN utility functions
'''

def pippo():
    print 'pippo'

# rdd1,rdd2 in the form ((row_idx, col_idx), value)
# computes the matrix-matrix product RDD1 * RDD2^t
def matmul_join(rdd1, rdd2):
    rdd1mult = rdd1.map(lambda x: (x[0][1], (x[0][0], x[1])))
    rdd2mult = rdd2.map(lambda x: (x[0][1], (x[0][0], x[1])))
    return rdd1mult.join(rdd2mult) \
        .map(lambda x: ((x[1][0][0], x[1][1][0]), x[1][0][1] * x[1][1][1])) \
        .reduceByKey(lambda x, y: x+y)

'''
Load/Save utility functions
'''

def loadDataset(config):
    inFile = path.join(config['general']['bucketName'], config['general']['clientname'])
    # trainBatch = sc.textFile(path.join(inFile, 'train/batchTraining'))
    # trainBatch = (sc
    #               .textFile(path.join(inFile, 'train/batchTraining'))
    #               .filter(lambda x: int(json.loads(x)['ts']) < config['split']['ts']))
    # testBatch = sc.textFile(path.join(inFile, 'test/batchTraining'))
    # testOnline = sc.textFile(path.join(inFile, 'test/onlineTraining'))
    testBatch = sc.textFile(path.join(inFile, 'test/batchTraining'))
    testOnline = sc.textFile(path.join(inFile, 'test/onlineTraining'))
    batch = testBatch
    return batch, testOnline



def saveRecommendations(conf, recJsonRdd, overwrite=False):
    algo_conf = conf['algo']['name'] + '_' + \
                '#'.join([str(v) for k, v in conf['algo']['props'].iteritems()])
    algo_conf = re.sub(r'[^A-Za-z0-9#_]', '', algo_conf)
    base_path = path.join(conf['general']['clientname'], 'Rec', algo_conf)

    # save recommendations to S3 storage
    outfile = path.join(conf['general']['bucketName'], base_path, "recommendations")
    # recJsonRdd.repartition(10).saveAsTextFile(outfile)
    recJsonRdd.saveAsTextFile(outfile)
    print "Recommendations successfully written to %s" % outfile


def loadRecommendations(conf):
    algo_conf = conf['algo']['name'] + '_' + \
                '#'.join([str(v) for k, v in conf['algo']['props'].iteritems()])
    algo_conf = re.sub(r'[^A-Za-z0-9#_]', '', algo_conf)
    base_path = path.join(conf['general']['clientname'], 'Rec', algo_conf)

    # save recommendations to S3 storage
    inFile = path.join(conf['general']['bucketName'], base_path, "recommendations")

    return sc.textFile(inFile)




### This is used to remap the recommendationof clusters to the songs they contain ###

#Function to map ClusterID - > Recommandation
def extract_ids(x):
    result = []
    responses = x['linkedinfo']['response']
    for r in responses:
        result.append( (r['id'], (r['rank'], x)) )
    return result


#Function to substitute the recommended clusters with their songs
def plug_songs(x):
    rec_size = conf['split']['reclistSize']
    rec_dict = json.loads(x[0])
    rec_dict['linkedinfo']['respone'] = []
    sorted_songs = sorted(x[1], key = itemgetter(0))
    inserted = 0
    for i in sorted_songs:
        for j in i[1]:
            entry = {"type": "track", "id": j, "rank": inserted}
            rec_dict['linkedinfo']['respone'].append(entry)
            inserted += 1
            if inserted >= rec_size:
                return json.dumps(rec_dict)
    return json.dumps(rec_dict)

#Pick the song with highest ID
def plug_one_song(x):
    rec_size = conf['split']['reclistSize']
    rec_dict = json.loads(x[0])
    rec_dict['linkedinfo']['respone'] = []
    sorted_songs = sorted(x[1], key = itemgetter(0))
    inserted = 0
    for i in sorted_songs:
        songs = i[1]
        song = min(songs)
        entry = {"type": "track", "id": song, "rank": i[0]}
        rec_dict['linkedinfo']['respone'].append(entry)

    return json.dumps(rec_dict)
            

def mapClusterRecToListOfSongs(recRDD, clusterRDD):
    
    recFlatRDD = recRDD.flatMap(extract_ids)
    #We have ClusterID -> (rank, Rec) and we join with ClusterIdD -> [songs]
    recJoinRDD = recFlatRDD.join(clustersRDD)
    #We have now (ClusterID, ( (rank, Rec), [songs]) )
    recSub = recJoinRDD.map(lambda x: (json.dumps(x[1][0][1]), (x[1][0][0], x[1][1])))
    #We extract (Rec, rank, [songs]) and griup by Rec. Then we plug the songs
    recAgg = recSub.groupByKey().map(plug_one_song)
    
    recAgg.count()
    return recAgg