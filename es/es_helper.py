from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import hashlib


class ESClient(object):
    def __init__(self, **kwargs):
        """
        :arg user: es user
        :arg password: es user's password
        :arg hosts: str or List[str], ip
        """
        self.user = kwargs['user']
        self.password = kwargs['password']
        self.hosts = kwargs['hosts']
        self.index = self.get_index()
        self.mappings = self.get_mappings()
        self.es = Elasticsearch(hosts=self.hosts, http_auth=(self.user, self.password), port=9200, timeout=10000)

    def delete_index(self):
        """delete index"""
        return self.es.indices.delete(index=self.index)

    def build_index(self, overwrite=False):
        """build a new index"""
        if self.es.indices.exists(self.index):
            if not overwrite:
                print("The index \"%s\" already exists, are you sure to delete it? Set overwrite to true" % self.index)
                return None
            else:
                self.delete_index()
        self.es.indices.create(index=self.index, body=self.mappings)

    def search(self, query):
        """search"""
        return self.es.search(index=self.index, body=query, size=10)["hits"]["hits"]

    def bulk_docs(self, actions, chunk_size=1000):
        """批处理插入文档，actions中指定 _op_type 为 create 当_id存在时不插入"""
        success, _ = bulk(self.es,
                          actions=actions,
                          stats_only=True,
                          index=self.index,
                          chunk_size=chunk_size,
                          raise_on_exception=False,  # 插入数据失败时，不需要抛出异常
                          raise_on_error=False  # 不抛出BulkIndexError
                          )
        return success

    def index_doc(self, action, id=None):
        """插入单文档，模式为create"""
        try:
            self.es.index(index=self.index, doc_type="_doc", id=id, body=action, op_type='create')
            return 1
        except Exception as e:
            print(str(e))
            return 0

    def get_mappings(self):
        """get the mappings"""
        raise NotImplemented

    def get_index(self):
        """get index name"""
        raise NotImplemented


class PaperClient(ESClient):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)

    def get_index(self):
        return 'paper'

    def get_mappings(self):
        mappings = {
            "mappings": {
                "properties": {
                    "id": {
                        "type": "keyword",  # hash code
                        "index": "true"
                    },
                    "title": {
                        "type": "text",
                        "index": "true"
                    },
                    "authors": {
                        "type": "keyword",
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

    @staticmethod
    def update_action(source, is_bulk=False):
        "source dict()"
        md5 = hashlib.md5()
        text = '-'.join([source['title']]) + '-' + '-'.join(source['authors'])
        md5.update(text.encode('utf-8'))
        hashcode = md5.hexdigest()
        source = dict({"id": hashcode}, **source)
        if is_bulk:
            source = {"_id": hashcode, "_source": source}
        return source, hashcode


class TripletClient(ESClient):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)

    def get_index(self):
        return 'triplet'

    def get_mappings(self):
        mappings = {
            "mappings": {
                "properties": {
                    "id": {
                        "type": "keyword",  # hash code
                        "index": "true"
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

    @staticmethod
    def update_action(source, is_bulk=False):
        "source dict()"
        md5 = hashlib.md5()
        text = '-'.join([v for _, v in source.items()])
        md5.update(text.encode('utf-8'))
        hashcode = md5.hexdigest()
        source = dict({"id": hashcode}, **source)
        if is_bulk:
            source = {"_id": hashcode, "_source": source}
        return source, hashcode

class MetadataClient(ESClient):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)

    def get_index(self):
        return 'metadata'

    def get_mappings(self):
        mappings = {
            "mappings": {
                "properties": {
                    "id": {
                        "type": "keyword",  # hash code
                        "index": "true"
                    },
                    "s2Id": {
                        "type": "keyword",
                        "index": "false"
                    },
                    "title": {
                        "type": "text",
                        "index": "true"
                    },
                    "authors": {
                        "type": "keyword",
                        "index": "true"
                    },
                    "year": {
                        "type": "keyword",
                        "index": "true"
                    }
                }
            }
        }
        return mappings

    @staticmethod
    def update_action(source, is_bulk=False):
        "source dict()"
        md5 = hashlib.md5()
        text = '-'.join([source['title']]) + '-' + '-'.join(source['authors'])
        md5.update(text.encode('utf-8'))
        hashcode = md5.hexdigest()
        source = dict({"id": hashcode}, **source)
        if is_bulk:
            source = {"_id": hashcode, "_source": source}
        return source, hashcode
