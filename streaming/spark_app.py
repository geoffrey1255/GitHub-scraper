import sys
import requests
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import *
import json

def aggregate_count(new_values, total_sum):
    return 1

def get_sql_context_instance(spark_context):
    if('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SparkSession(spark_context)
    return globals()['sqlContextSingletonInstance']

def send_df_to_dashboard(df):
    url = 'http://webapp:5000/updateData'
    data = df.toPandas().to_dict('list')
    requests.post(url, json=data)

def send_df_to_dashboard2(df):
    url = 'http://webapp:5000/updateData2'
    data = df.toPandas().to_dict('list')
    requests.post(url, json=data)

def send_df_to_dashboard3(df):
    url = 'http://webapp:5000/updateData3'
    data = df.toPandas().to_dict('list')
    requests.post(url, json=data)

def send_df_to_dashboard4(df):
    url = 'http://webapp:5000/updateData4'
    data = df.toPandas().to_dict('list')
    requests.post(url, json=data)

def send_df_to_dashboardpy(df):
    url = 'http://webapp:5000/updateDatapy'
    data = df.toPandas().to_dict('list')
    requests.post(url, json=data)

def send_df_to_dashboardjv(df):
    url = 'http://webapp:5000/updateDatajv'
    data = df.toPandas().to_dict('list')
    requests.post(url, json=data)

def send_df_to_dashboardjs(df):
    url = 'http://webapp:5000/updateDatajs'
    data = df.toPandas().to_dict('list')
    requests.post(url, json=data)

def part1_rdd(time, rdd):
    pass
    print("----------- Secontion 1 -----------")
    print("----------- %s -----------" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
 
        row_rdd = rdd.map(lambda w: Row(FullName=w[0][0], Language=w[0][1], Star=w[0][2], Count=w[1]))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("results")
        new_results_df = sql_context.sql("select FullName, Language, Star, count from results order by Count")

        new_results_df.groupBy("Language").sum("Count").show()
        new_df = new_results_df.groupBy("Language").sum("Count")
        send_df_to_dashboard(new_df)
        
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)
        
def part2_rdd(time, rdd):
    pass
    print("----------- Section 2 -----------")
    print("----------- %s -----------" % str(time))
    try: 
        sql_context = get_sql_context_instance(rdd.context)
 
        row_rdd = rdd.map(lambda w: Row(FullName=w[0][0], Language=w[0][1], Star=w[0][2], Pushed=w[0][4], Count=w[1]))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("results")
        new_results_df = sql_context.sql("select FullName, Language, Star, Pushed, count from results order by Count")

        reposix = new_results_df.withColumn("timestamp", current_timestamp().cast("long")-to_timestamp("Pushed").cast("long"))

        
        print("Number of repository with changes pushed within 60seconds: ", reposix.where("timestamp <= 60").count())
        print("\n")
        usix = reposix.where("timestamp <= 60")
        test = usix.groupBy("Language").sum("Count")
        
        
        send_df_to_dashboard2(test)
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

def part3_rdd(time, rdd):
    pass
    print("----------- Section 3 -----------")
    print("----------- %s -----------" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
 
        row_rdd = rdd.map(lambda w: Row(FullName=w[0][0], Language=w[0][1], Star=w[0][2], Count=w[1]))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("results")
        new_results_df = sql_context.sql("select FullName, Language, Star, count from results order by Count")
        new_results_df.groupBy("Language").avg("Star").show()
        
        dfavg = new_results_df.groupBy("Language").avg("Star")
        send_df_to_dashboard3(dfavg)
        
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


def part4_rdd(time, rdd):
    print("----------- Secontion 4 -----------")
    print("----------- %s -----------" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(Words=w[0], Count=w[1]))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("results")
        new_results_df = sql_context.sql("select Words, count from results order by Count")

        new_results_df.orderBy("Count", ascending = [False]).show(10)
        tt = new_results_df.orderBy("Count", ascending = [False]).limit(10)
        send_df_to_dashboard4(tt)

    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

def pyrdd(time, rdd):
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(Words=w[0], Count=w[1]))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("results")
        new_results_df = sql_context.sql("select Words, count from results order by Count")

        pytop = new_results_df.orderBy("Count", ascending = [False]).limit(10)
        send_df_to_dashboardpy(pytop)

def jvrdd(time, rdd):
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(Words=w[0], Count=w[1]))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("results")
        new_results_df = sql_context.sql("select Words, count from results order by Count")

        jvtop = new_results_df.orderBy("Count", ascending = [False]).limit(10)
        send_df_to_dashboardjv(jvtop)

def jsrdd(time, rdd):
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(Words=w[0], Count=w[1]))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("results")
        new_results_df = sql_context.sql("select Words, count from results order by Count")

        jstop = new_results_df.orderBy("Count", ascending = [False]).limit(10)
        send_df_to_dashboardjs(jstop)
        
if __name__ == "__main__":
    DATA_SOURCE_IP = "data-source"
    DATA_SOURCE_PORT = 9999
    sc = SparkContext(appName="NineMultiples")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 30)
    ssc.checkpoint("checkpoint_NineMultiples")
    data = ssc.socketTextStream(DATA_SOURCE_IP, DATA_SOURCE_PORT)
    
    info = data.map(lambda s: json.loads(s))

    datajson=info.map(lambda s: ((s["full_name"], s["language"], s["stargazers_count"], s["description"], s["pushed_at"]), 1)).reduceByKey(lambda x, y: 1)

    
    aggregated_counts = datajson.updateStateByKey(aggregate_count)
    aggregated_counts.foreachRDD(part1_rdd)
    aggregated_counts.foreachRDD(part2_rdd)
    aggregated_counts.foreachRDD(part3_rdd)


    
    dataword = aggregated_counts.flatMap(lambda s: "" if s[0][3] is None else s[0][3].split(" "))
    pyword = aggregated_counts.flatMap(lambda s: "" if s[0][3] is None else (s[0][3].split(" ") if s[0][1] == "Python" else ""))
    jvword = aggregated_counts.flatMap(lambda s: "" if s[0][3] is None else (s[0][3].split(" ") if s[0][1] == "Java" else ""))
    jsword = aggregated_counts.flatMap(lambda s: "" if s[0][3] is None else (s[0][3].split(" ") if s[0][1] == "JavaScript" else ""))


    data_fw = dataword.filter(lambda x: x.strip() != "")
    py_fw = pyword.filter(lambda x: x.strip() != "")
    jv_fw = jvword.filter(lambda x: x.strip() != "")
    js_fw = jsword.filter(lambda x: x.strip() != "")
    data_rdd = data_fw.map(lambda s: (s,1)).reduceByKey(lambda x, y: x+y)
    py_rdd = py_fw.map(lambda s: (s,1)).reduceByKey(lambda x, y: x+y)
    jv_rdd = jv_fw.map(lambda s: (s,1)).reduceByKey(lambda x, y: x+y)
    js_rdd = js_fw.map(lambda s: (s,1)).reduceByKey(lambda x, y: x+y)
    data_rdd.foreachRDD(part4_rdd)
    py_rdd.foreachRDD(pyrdd)
    jv_rdd.foreachRDD(jvrdd)
    js_rdd.foreachRDD(jsrdd)
    
    ssc.start()
    ssc.awaitTermination()

