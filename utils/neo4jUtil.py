# -*- coding: utf-8 -*-
from py2neo import Graph, authenticate

class Neo4jUtil(object):

    def __init__(self, url, username, password):

        # 验证参数需要去掉http前缀
        httpPrefix = "http://"
        # 设置登陆验证参数
        authenticate(url.replace(httpPrefix, ""), username, password)
        # 连接neo4j数据库
        self.graph = Graph(url + '/db/data/')

    def empty(self):
        self.graph.run("match (n) detach delete n")
    # 创建节点
    def create_column_node(self, node_name):
        match_result = self.graph.run("MATCH (a:Column) WHERE a.name = '%s' RETURN a" % node_name).data()
        if len(match_result) == 0:
            self.graph.run("CREATE (n:Column { name: '%s' })" % node_name)
    # 创建关系
    def create_column_relation(self, origin_name, dest_name):
        match_result = self.graph.run(
            "MATCH (a:Column)-[r:beDepColumn]->(b:Column) WHERE a.name = '%s' AND b.name = '%s' RETURN r" %
            (origin_name, dest_name)).data()
        if len(match_result) == 0:
            self.graph.run("""MATCH (a:Column),(b:Column)
                            WHERE a.name = '%s' AND b.name = '%s'
                            CREATE (a)-[r:beDepColumn]->(b)""" % (origin_name, dest_name))
    # 清空库
    def delete_all(self):
        self.graph.delete_all()