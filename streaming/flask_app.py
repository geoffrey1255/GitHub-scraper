"""
    This Flask web app provides a very simple dashboard to visualize the statistics sent by the spark app.
    The web app is listening on port 5000.
    All apps are designed to be run in Docker containers.
    
    Made for: EECS 4415 - Big Data Systems (Department of Electrical Engineering and Computer Science, York University)
    Author: Changyuan Lin

"""


from flask import Flask, jsonify, request, render_template
from redis import Redis
import matplotlib.pyplot as plt
import json
import datetime

app = Flask(__name__)

recent5py = []
recent5jv = []
recent5js = []

@app.route('/updateData', methods=['POST'])
def updateData():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('data', json.dumps(data))
    return jsonify({'msg': 'success'})

@app.route('/updateData2', methods=['POST'])
def updateData2():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('data2', json.dumps(data))
    return jsonify({'msg': 'success'})

@app.route('/updateData3', methods=['POST'])
def updateData3():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('data3', json.dumps(data))
    return jsonify({'msg': 'success'})

@app.route('/updateData4', methods=['POST'])
def updateData4():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('data4', json.dumps(data))
    return jsonify({'msg': 'success'})

@app.route('/updateDatapy', methods=['POST'])
def updateDatapy():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('datapy', json.dumps(data))
    return jsonify({'msg': 'success'})

@app.route('/updateDatajv', methods=['POST'])
def updateDatajv():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('datajv', json.dumps(data))
    return jsonify({'msg': 'success'})

@app.route('/updateDatajs', methods=['POST'])
def updateDatajs():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('datajs', json.dumps(data))
    return jsonify({'msg': 'success'})

@app.route('/', methods=['GET'])
def index():
    r = Redis(host='redis', port=6379)
    data = r.get('data')
    data2 = r.get('data2')
    data3 = r.get('data3')
    data4 = r.get('data4')
    datapy = r.get('datapy')
    datajv = r.get('datajv')
    datajs = r.get('datajs')
    try:
        data = json.loads(data)
        data2 = json.loads(data2)
        data3 = json.loads(data3)
        data4 = json.loads(data4)
        datapy = json.loads(datapy)
        datajv = json.loads(datajv)
        datajs = json.loads(datajs)
    except TypeError:
        return "waiting for data..."
    try:
        py_index = data['Language'].index("Python")
        jv_index = data['Language'].index("Java")
        js_index = data['Language'].index("JavaScript")
        data1py_count = data['sum(Count)'][py_index]
        data1jv_count = data['sum(Count)'][jv_index]
        data1js_count = data['sum(Count)'][js_index]

##        if data2[
##        data2py_count = data2['sum(Count)'][py_index]
##        data2jv_count = data2['sum(Count)'][jv_index]
##        data2js_count = data2['sum(Count)'][js_index]
        
        data3py_count = data3['avg(Star)'][py_index]
        data3jv_count = data3['avg(Star)'][jv_index]
        data3js_count = data3['avg(Star)'][js_index]

        pylist= datapy['Words']
        jvlist= datajv['Words']
        jslist= datajs['Words']
        pyc=datapy['count']
        jvc=datajv['count']
        jsc=datajs['count']
    except ValueError:
        data1py_count = 0
        data1jv_count = 0
        data1js_count = 0
        data3py_count = 0
        data3jv_count = 0
        data3js_count = 0
        data2py_count = 0
        data2jv_count = 0
        data2js_count = 0

        
    

    height = [data3py_count, data3jv_count, data3js_count]
    tick_label = ['Python', 'Java', 'JavaScript']
    x = [1, 2, 3]
    plt.bar(x, height, tick_label=tick_label, width=0.8, color=['tab:blue', 'tab:orange', 'tab:green'])
    plt.ylabel('Repositories')
    plt.xlabel('Time')
    plt.title('Part 2')
    plt.legend()
    plt.savefig('/streaming/static/images/part2.png')

    plt.figure()
    x = [1, 2, 3]
    height = [data3py_count, data3jv_count, data3js_count]
    tick_label = ['Python', 'Java', 'JavaScript']
    plt.bar(x, height, tick_label=tick_label, width=0.8, color=['tab:orange', 'tab:gray', 'tab:red'])
    plt.ylabel('Average number of stars')
    plt.xlabel('PL')
    plt.title('Part 3')
    plt.savefig('/streaming/static/images/part3.png')
        
    
    return render_template('assignment3.html', url='/static/images/part2.png',urp='/static/images/part3.png',
                           pycount=data1py_count, jvcount=data1jv_count, jscount = data1js_count, jvlist=jvlist, pylist=pylist, jslist=jslist, pyc=pyc,jsc=jsc,jvc=jvc,
                           jnjpy=zip(pylist,pyc), jnjjs=zip(jslist,jsc), jnjjv=zip(jvlist,jvc))

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')
