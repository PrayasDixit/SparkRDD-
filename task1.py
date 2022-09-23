from pyspark.context import SparkContext
import json
import datetime as datetime
import time
import sys

sc = SparkContext.getOrCreate()

input_file_path=sys.argv[1]
output_file_path=sys.argv[2]

data_RDD=sc.textFile(input_file_path)
data=data_RDD.map(json.loads)
# Task 1
task1=data_RDD.map(json.loads).map(lambda kv: kv['date']).count()

# Task 2

date_data=data.map(lambda kv: kv['date'])
year_data=date_data.map(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S").year)
task2=year_data.filter(lambda x:x==2018).count()

# Task 3

task3=data.map(lambda x: x['user_id']).distinct().count()

# Task 4

grp_val=data.map(lambda x: [x['user_id'],1]).groupByKey()
rdd_freq=grp_val.mapValues(sum)
data_tup=rdd_freq.sortBy(lambda x:[-x[1],x[0]])
task4=data_tup.map(lambda x:list(x)).take(10)

# Task 5
task5=data.map(lambda x: x['business_id']).distinct().count()

# Task 6


grp_val=data.map(lambda x: [x['business_id'],1]).groupByKey()
rdd_freq=grp_val.mapValues(sum)
data_tup=rdd_freq.sortBy(lambda x:[-x[1],x[0]])
task6=data_tup.map(lambda x:list(x)).take(10)

# Preparing final output

json_dict=dict()
json_dict['n_review']=task1
json_dict['n_review_2018']=task2
json_dict['n_user']=task3
json_dict['top10_user']=task4
json_dict['n_business']=task5
json_dict['top10_business']=task6

final_json=json.dumps(json_dict)

with open(output_file_path, "w") as outfile:
    json.dump(json_dict, outfile)
