from solr import Solr
import csv
import sys

def generate_solr_updates(csv_file, min_confidence):
    '''
    Generates Solr atomic updates
    :param csv_file: csv file with fields
    :return: stream of atomic updates
    '''
    with open(csv_file) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            items = row.items()
            items = map(lambda kv: (kv[0],  kv[1] if kv[0] == 'path' else float(kv[1])), items) # to float
            items = filter(lambda kv: kv[0] == 'path' or kv[1] >= min_confidence, items)  # drop less confident objects
            row = dict(items)
            delta = {'id' : 'file:' + row['path']}
            del row['path']
            objects, scores = None, None # setting None will clear the existing value
            if row: # some detected object
                objects = []
                scores = []
                for obj, confd in row.items():
                    for o in obj.split(","):
                        objects.append(o.strip())
                        scores.append(confd)
            delta['objects'] = {'set' : objects}
            delta['confidence'] = {'set': scores}
            yield delta


if __name__ == '__main__':
    # Get the CSV file from classifier-local.py
    if len(sys.argv) != 2:
        print("required args:\n <CSV_file>")
        sys.exit(1)
    infile = sys.argv[1]
    min_confidence = 0.30
    print("Reading from %s, Min confidence=%f" % (infile, min_confidence))
    solr_url = "http://localhost:8983/solr/imagecatdev"
    solr = Solr(solr_url)

    updates = generate_solr_updates(infile, min_confidence=min_confidence)
    count, res = solr.post_iterator(updates, commit=True, buffer_size=1000)
    print("Res : %s; count=%d" %(res, count))
    '''
    from pprint import pprint
    for u in updates:
      pprint(u)
    '''
