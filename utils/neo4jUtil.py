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
        # 查询该节点是否已经存在
        node_info_list = str(node_name).split(".")
        if(3 != len(node_info_list)):
            return

        label_str, node_str = self.get_label_and_node(node_info_list)

        # {0}是节点的标签名称：库名+表名
        # {1}是节点的属性名称：字段名
        match_result = self.graph.run("MATCH (a:{0}) WHERE a.name ='{1}' RETURN a".format(label_str, node_str)).data()
        if len(match_result) == 0:
            # rec: 插入时节点时，label名称不能加引号，属性值需要加引号
            self.graph.run("CREATE (n:%s { name: '%s' })" % (label_str, node_str))
    # 创建关系
    def create_column_relation(self, origin_name, dest_name, relation_name):

        origin_info_list = str(origin_name).split(".")
        dest_info_list = str(dest_name).split(".")

        if(3 != len(origin_info_list) or 3 != len(dest_info_list)):
            return

        origin_label, origin_node = self.get_label_and_node(origin_info_list)

        dest_label, dest_node = self.get_label_and_node(dest_info_list)

        # 查询该关系是否已经存在
        match_result = self.graph.run(
            "MATCH (a:{0})-[r:{4}]->(b:{1}) WHERE a.name = '{2}' AND b.name = '{3}' RETURN r".format(origin_label, dest_label, origin_node, dest_node, relation_name)).data()
        if len(match_result) == 0:
            self.graph.run("""MATCH (a:{0}),(b:{1})
                            WHERE a.name = '{2}' AND b.name = '{3}'
                            CREATE (a)-[r:{4}]->(b)""".format(origin_label, dest_label, origin_node, dest_node, relation_name))

    def get_label_and_node(self, node_info_list):
        label_name_str = node_info_list[0] + "___" + node_info_list[1]
        node_name_str = node_info_list[2]
        return label_name_str, node_name_str

    # 清空库
    def delete_all(self):
        self.graph.delete_all()