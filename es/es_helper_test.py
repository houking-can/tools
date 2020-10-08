from es_helper import TripletClient, PaperClient

def test_build_tc_index():
    # 建立索引
    client = TripletClient(user='elastic',password='elastic123',hosts='10.1.114.114')
    client.build_index()
    IS_BULK = True

    # 插入数据
    triplet1_action = {"headEntity": "CNN", "headEntityType": "METHOD", "tailEntity": "classification",
                       "tailEntityType": "TASK", "relationType": "USED-FOR"}
    triplet1_action, id1 = client.update_action(triplet1_action, is_bulk=IS_BULK)
    triplet2_action = {"headEntity": "RNN", "headEntityType": "METHOD", "tailEntity": "classification",
                       "tailEntityType": "TASK", "relationType": "USED-FOR"}
    triplet2_action, id2 = client.update_action(triplet2_action, is_bulk=IS_BULK)

    triplet3_action = {"headEntity": "RNN", "headEntityType": "METHOD", "tailEntity": "classification",
                       "tailEntityType": "TASK", "relationType": "USED-FOR"}
    triplet3_action, id3 = client.update_action(triplet3_action)

    actions = [triplet1_action, triplet2_action, triplet2_action]
    print(client.bulk_docs(actions=actions))

    # 测试插入单条
    print(client.index_doc(action=triplet3_action, id=id3))


def test_build_pc_index():
    # 建立索引
    client = PaperClient(user='elastic',password='elastic123',hosts='10.1.114.114')
    client.build_index()
    IS_BULK = True

    # 插入数据
    triplet1_action = {"title": "CNN", "authors": ["jp","yhj"], "affiliations": ["bit","cmu"],
                       "emails": ["123@bit.edu.cn","567@bit.edu.cn"], "keywords": ["NER","NLP"],
                       "abstract":"We present CL Scholar, the ACL Anthology knowledge graph miner to facilitate highquality search and exploration of current research progress in the computational linguistics community. In contrast to previous works, periodically crawling, indexing and processing of new incoming articles is completely automated in the current system. CL Scholar utilizes both textual and network information for knowledge graph construction. As an additional novel initiative, CL Scholar supports more than 1200 scholarly natural language queries along with standard keywordbased search on constructed knowledge graph.",
                       "bookmarks":["Introduction","Experiments"],"paperText":["Hello","See you."],
                       "references.title":"A robust domain-specific NER approach",
                       "references.authors":["wh","wsf"],
                       "references.journal":["IJCAI-2020"]}
    triplet1_action, id1 = client.update_action(triplet1_action, is_bulk=IS_BULK)
    triplet2_action = {"title": "CRNN", "authors": ["jp","yhj"], "affiliations": ["bit","cmu"],
                       "emails": ["123@bit.edu.cn","567@bit.edu.cn"], "keywords": ["NER","NLP"],
                       "abstract":"We present CL Scholar, the ACL Anthology knowledge graph miner to facilitate highquality search and exploration of current research progress in the computational linguistics community. In contrast to previous works, periodically crawling, indexing and processing of new incoming articles is completely automated in the current system. CL Scholar utilizes both textual and network information for knowledge graph construction. As an additional novel initiative, CL Scholar supports more than 1200 scholarly natural language queries along with standard keywordbased search on constructed knowledge graph.",
                       "bookmarks":["Introduction","Experiments"],"paperText":["Hello","See you."],
                       "references":[{"title":"A robust domain-specific NER approach","authors":["whj","wsf"],"journal":["IJCAI-2020"]}]}
    triplet2_action, id2 = client.update_action(triplet2_action, is_bulk=IS_BULK)

    actions = [triplet1_action, triplet2_action]
    print(client.bulk_docs(actions=actions))


def test_search():
    TC = TripletClient()
    res = TC.search(query={
        "query":
            {"match":
                 {"headEntity": "RNN"}
             }
    })
    print(res)


if __name__ == "__main__":
    pass
    # test_build_pc_index()
    # test_search()
