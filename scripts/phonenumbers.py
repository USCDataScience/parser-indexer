from solr import Solr
import json


def fix_phonenumbers(docs):
    '''
    Removes last modified value from from docs
    :param docs:
    :return:
    '''
    
    for d in docs:
        u = {'id': d['id']}
        if 'ner_phone_number_t_md' in d:
            u['phonenumbers'] = {'set': d['ner_phone_number_t_md']}
            u['ner_phone_number_t_md'] = {'set': None}
        elif 'ner_phone_number_ts_md' in d:
            u['phonenumbers'] = {'set': d['ner_phone_number_ts_md']}
            u['ner_phone_number_ts_md'] = {'set': None}
        else:
            print("Error: Skipped")
            continue
        yield u


def read_stream(filename):
    '''
    Reads json line stream
    :param filename: path to json line
    :return: doc stream
    '''
    with open(filename) as inf:
        for l in inf:
            yield json.loads(l)

if __name__ == '__main__':
    url = "http://127.0.0.1:8983/solr/imagecatdev"
    solr = Solr(url)
    docs = solr.query_iterator("ner_phone_number_t_md:* OR ner_phone_number_ts_md:*",
                        rows=1000, fl='id,ner_phone_number_t_md,ner_phone_number_ts_md', sort="indexedAt asc")

    updates = fix_phonenumbers(docs)
    count, success = solr.post_iterator(updates, False, buffer_size=1000)
    solr.commit()
    print(success)
    print(count)

