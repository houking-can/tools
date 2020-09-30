from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


class ESClient(object):
    def __init__(self):
        self.user = 'elastic'
        self.password = 'elastic123'
        self.hosts = ['10.1.114.114']
        self.index = self.get_index()
        self.mappings = self.get_mappings()
        self.es = Elasticsearch(hosts=self.hosts, http_auth=(self.user, self.password), port=9200, timeout=10000)

    def delete_index(self):
        return self.es.indices.delete(index=self.index)

    def build_index(self, overwrite=False):
        if self.es.indices.exists(self.index):
            if not overwrite:
                print("The index \"%s\" already exists, are you sure to delete it? Set overwrite to true" % self.index)
                return None
            else:
                self.delete_index()
        # build a new index
        self.es.indices.create(index=self.index, body=self.mappings)

    def get_mappings(self):
        raise NotImplemented

    def get_index(self):
        raise NotImplemented


class PaperClient(ESClient):
    def __init__(self):
        super().__init__()

    def get_index(self):
        return 'paper'

    def get_mappings(self):
        mappings = {
            "mappings": {
                "properties": {
                    "paperId": {
                        "type": "long",
                        "index": "false"
                    },
                    "uniqueId": {
                        "type": "keyword",  # hash code
                        "index": "false"
                    },
                    "title": {
                        "type": "text",
                        "index": "true"
                    },
                    "authors": {
                        "type": "text",
                        "index": "true"
                    },
                    "affiliations": {
                        "type": "text",
                        "index": "true"
                    },
                    "emails": {
                        "type": "keyword",
                        "index": "false"
                    },
                    "keywords": {
                        "type": "keyword",
                        "index": "true"
                    },
                    "abstract": {
                        "type": "text",
                        "index": "true"
                    },
                    "bookmarks": {
                        "type": "text",
                        "index": "false"
                    },
                    "paperText": {
                        "type": "text",
                        "index": "false"
                    },
                    "references": {  # tags可以存json格式，references.content
                        "type": "object",
                        "properties": {
                            "title": {"type": "text", "index": "false"},
                            "authors": {"type": "text", "index": "false"},
                            "journal": {"type": "text", "index": "false"}
                        }
                    }
                }
            }
        }
        return mappings

    def search(self):
        pass

    def bulk_write(self):
        pass


class TripletClient(ESClient):
    def __init__(self):
        super().__init__()
        self.index = 'triplet'

    def get_index(self):
        return 'triplet'

    def get_mappings(self):
        mappings = {
            "mappings": {
                "properties": {
                    "tripletId": {
                        "type": "long",
                        "index": "false"
                    },
                    "uniqueId": {
                        "type": "keyword",  # hash code
                        "index": "false"
                    },
                    "headEntity": {
                        "type": "text",
                        "index": "true"
                    },
                    "headEntityType": {
                        "type": "keyword",
                        "index": "false"
                    },
                    "tailEntity": {
                        "type": "text",
                        "index": "true"
                    },
                    "tailEntityType": {
                        "type": "keyword",
                        "index": "false"
                    },
                    "relationType": {
                        "type": "keyword",
                        "index": "false"
                    },
                }
            }
        }
        return mappings

    def search(self):
        pass

    def bulk_write(self):
        pass


if __name__ == "__main__":
    PC = PaperClient()
    PC.build_index()
    # if PC.es.indices.exists("hello"):
    #     b=PC.es.indices.delete(index='hello')
    #     print(b)
