# -*- coding: utf-8 -*-
from flask import Flask, request
import json

from showData.process import process_lineage_hook_info, clear_all

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

@app.route('/create_graph', methods=["POST"])
def parse_post():
    # 1、request.form['***'], 获取POST请求传过来的from-data数据
    # postdata = request.form['test']

    # 2、request.get_data(as_text=True), 获取POST请求传过来的 json 数据
    post_dict = json.loads(request.get_data(as_text=True))
    process_lineage_hook_info(post_dict)

    return "数据插入成功！"

@app.route('/clear_all', methods=["GET"])
def parse_get():

    clear_all()

    return "清空数据成功！"


if __name__ == '__main__':

    # app.run("0.0.0.0", port=8888)
    app.run()
