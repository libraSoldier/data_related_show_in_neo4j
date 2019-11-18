# -*- coding: utf-8 -*-

# @Author: liangyu
# @Date  : 2019/11/15 3:28 下午
from utils import neo4jUtil

# line 是打印出的hook log
def process_lineage_hook_info(json_dict):

    # lineage_info = line.strip().split("hooks.LineageLogger:")[1]
    # data_dict = json.loads(lineage_info)
    data_dict = json_dict

    # rec: 顶点dict
    vertices = data_dict["vertices"]
    vertices_dict = {}
    for elem in vertices:
        id = elem["id"] # 边id
        vertex_type = elem["vertexType"] # 边类型
        vertex_id = elem["vertexId"] # 字段名称
        vertices_dict[id] = (vertex_type, vertex_id)

    # rec: 边dict
    edges = data_dict["edges"]
    for elem in edges:
        sources = elem["sources"]
        targets = elem["targets"]
        edgeType = elem["edgeType"]
        expression = ""
        if "expression" in elem:
            expression = elem["expression"]

        if edgeType == "PROJECTION":
            do_insert(vertices_dict, expression, sources, targets, "PROJECTION")
        elif edgeType == "PREDICATE":
            do_insert(vertices_dict, expression, sources, targets, "PREDICATE")

        # 不考虑 join + group by 的情况，要不然数据量过多
        # if edgeType == "PROJECTION":
        #     do_insert(vertices_dict, expression, sources, targets)
        # if edgeType == "PREDICATE":
        #     # 处理一对多的关系
        #     if len(sources) == 1 and len(targets) > 1:
        #         do_insert(vertices_dict, expression, sources, targets)
        #     elif len(sources) > 1 and len(targets) > 1:
        #         do_insert(vertices_dict, expression, sources, targets)

# 登陆neo4j
def login_neo4j():
    url = "http://localhost:7474"
    user_name = "neo4j"
    pass_word = "deploy"
    # 实例化类
    return neo4jUtil.Neo4jUtil(url, user_name, pass_word)

def do_insert(vertices_dict, expression, sources, targets, relation_name):
    for source in sources:
        if vertices_dict[source][0] == "COLUMN":
            origin_column_name = vertices_dict[source][1]
        elif vertices_dict[source][0] == "TABLE":
            origin_column_name = vertices_dict[source][1] + "." + expression
        for target in targets:
            if vertices_dict[target][0] == "COLUMN":
                dest_column_name = vertices_dict[target][1]
            elif vertices_dict[target][0] == "TABLE":
                dest_column_name = vertices_dict[target][1] + "." + expression
            neo4j_insert(origin_column_name, dest_column_name, relation_name)

def neo4j_insert(origin_column_name, dest_column_name, relation_name):
    neo4jUtilObj = login_neo4j()
    neo4jUtilObj.create_column_node(origin_column_name)
    neo4jUtilObj.create_column_node(dest_column_name)
    neo4jUtilObj.create_column_relation(origin_column_name, dest_column_name, relation_name)

def clear_all():
    neo4jUtilObj = login_neo4j()
    neo4jUtilObj.delete_all()


if __name__ == '__main__':

    dict1 = {"edges":[{"sources":[2],"targets":[0],"edgeType":"PROJECTION"},{"sources":[3],"targets":[1],"expression":"max(ods.test_table.c2)","edgeType":"PROJECTION"}],"vertices":[{"id":0,"vertexType":"COLUMN","vertexId":"c1"},{"id":1,"vertexType":"COLUMN","vertexId":"max_c2"},{"id":2,"vertexType":"COLUMN","vertexId":"ods.test_table.c1"},{"id":3,"vertexType":"COLUMN","vertexId":"ods.test_table.c2"}]}

    dict2 = {"version":"1.0","user":"hadoop","timestamp":1510308124,"duration":45959,"jobIds":["job_1509088410884_16752"],"engine":"mr","database":"cxy7_dw","hash":"a184be21aadb9dd5b6c950fe0b3298d8","queryText":"SELECT z.zoneid AS zone_id,z.zonename AS zone_name, c.cityid AS city_id, c.cityname AS city_name FROM dict_zoneinfo z LEFT JOIN dict_cityinfo c ON z.cityid = c.cityid AND z.dt='20171109' AND c.dt='20171109' WHERE z.dt='20171109' AND c.dt='20171109' LIMIT 10","edges":[{"sources":[4],"targets":[0],"edgeType":"PROJECTION"},{"sources":[5],"targets":[1],"edgeType":"PROJECTION"},{"sources":[6],"targets":[2],"edgeType":"PROJECTION"},{"sources":[7],"targets":[3],"edgeType":"PROJECTION"},{"sources":[8,6],"targets":[0,1,2,3],"expression":"(z.cityid = c.cityid)","edgeType":"PREDICATE"},{"sources":[9],"targets":[0,1,2,3],"expression":"(c.dt = '20171109')","edgeType":"PREDICATE"},{"sources":[10,9],"targets":[0,1,2,3],"expression":"((z.dt = '20171109') and (c.dt = '20171109'))","edgeType":"PREDICATE"}],"vertices":[{"id":0,"vertexType":"COLUMN","vertexId":"zone_id"},{"id":1,"vertexType":"COLUMN","vertexId":"zone_name"},{"id":2,"vertexType":"COLUMN","vertexId":"city_id"},{"id":3,"vertexType":"COLUMN","vertexId":"city_name"},{"id":4,"vertexType":"COLUMN","vertexId":"cxy7_dw.dict_zoneinfo.zoneid"},{"id":5,"vertexType":"COLUMN","vertexId":"cxy7_dw.dict_zoneinfo.zonename"},{"id":6,"vertexType":"COLUMN","vertexId":"cxy7_dw.dict_cityinfo.cityid"},{"id":7,"vertexType":"COLUMN","vertexId":"cxy7_dw.dict_cityinfo.cityname"},{"id":8,"vertexType":"COLUMN","vertexId":"cxy7_dw.dict_zoneinfo.cityid"},{"id":9,"vertexType":"COLUMN","vertexId":"cxy7_dw.dict_cityinfo.dt"},{"id":10,"vertexType":"COLUMN","vertexId":"cxy7_dw.dict_zoneinfo.dt"}]}

    process_lineage_hook_info(dict2)
