import os
from es_helper import TripletClient, PaperClient, MetadataClient
import glob
import json

def index_paper():
    pass

def index_triplet():
    pass

def index_metadata():
    client = MetadataClient(user='elastic',password='elastic123',hosts='127.0.0.1')
    client.build_index()
    for file in glob.glob('/data/yhj/dataset/abstract/*.jsonl'):
        lines = open(file,encoding='utf-8').read().strip().split('\n')
        actions = []
        for line in lines:
            line = json.loads(line)
            line.pop('doi')
            line.pop('paperAbstract')
            if not line['title'] or not line['authors']:
                continue
            line,_ = client.update_action(line, is_bulk=True)
            actions.append(line)
        success = client.bulk_docs(actions)
        client.es.indices.refresh(client.index)
        print(file, success)

if __name__=="__main__":
    index_metadata()
    pass
