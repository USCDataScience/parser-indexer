# This script aggregates the objects from images to the connected ads

from solr import Solr, current_milli_time
import sys, os, csv, time, pickle, hashlib, math
from collections import defaultdict as ddict
from pprint import pprint

log_delay = 2000

def hash_string(s):
    return int(hashlib.sha1(s).hexdigest(), 16)

def cache_image_data(solr, start=0, rows=5000):
    data = {}
    imgs = solr.query_iterator(query="mainType:image AND objects:*", start=start,
                    rows=rows, fl='id,objects,confidence,indexedAt', sort="indexedAt asc")
    t = current_milli_time()
    count = 0
    for img in imgs:
        # converting long url strings numbers to save some memory
        data[hash_string(img['id'])] = [img.get('objects'), img.get('confidence')]
        count += 1
        if current_milli_time() - t > log_delay:
            t = current_milli_time()
            print('%d :: %d :: %s' % (count, t, img['indexedAt']))
    return data

def aggregate(obj_confs):
    '''
        aggregates based on the maximum confidence of each objects seen in the linked images
    '''
    result = ddict(float)
    for obj_conf in obj_confs:
        objs, confs = obj_conf[0], obj_conf[1]
        if len(objs) != len(confs):
            print("Error: no one-to-one mapping")
            continue
        for i, obj in enumerate(objs):
            result[obj] = max(result[obj], confs[i])
    return result

def generate_solr_updates(solr, imgdb):
    # outpaths:*&fl=id,outpaths,indexedAt&sort=indexedAt%20asc
    docs = solr.query_iterator(query="mainType:text OR contentType:/.*ml.*/", fl="id,outpaths", sort="indexedAt asc", rows=500)
    # filter out the docs without outlinks
    docs = filter(lambda x: 'outpaths' in x and x['outpaths'],  docs)
    for doc in docs:
        children = map(hash_string, doc['outpaths']) # hash the strings
        images = list(filter(lambda x: x in imgdb, children)) # filter the images which are there in our cache
        if images:
            object_confs = map(lambda x: imgdb[x], images)
            #print(doc['id'])
            res = aggregate(object_confs)
            objs, confs = sorted(res, key=res.get, reverse=True), sorted(res.values(), reverse=True)
            yield {
                'id': doc['id'],
                'confidence': {'set': confs},
                'objects': {'set': objs}
            }

if __name__ == '__main__':

    datafile = 'imagedata.pickle'
    indexedAt = None
    solr_url = "http://localhost:8983/solr/imagecatdev"
    solr = Solr(solr_url)
    if not os.path.exists(datafile):
        data = cache_image_data(solr)
        pickle.dump(data, open(datafile, 'wb'))
    else:
        print("Restoring the cache")
        data = pickle.load(open(datafile, 'rb'))
        print("Done")
    updates = generate_solr_updates(solr, data)
    count, res = solr.post_iterator(updates, commit=True, buffer_size=1000)
    print("Res : %s; count=%d" %(res, count))
