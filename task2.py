from pyspark.context import SparkContext
import json
import datetime as datetime
import time
import sys

sc = SparkContext.getOrCreate()

input_file_path=sys.argv[1]
output_file_path=sys.argv[2]
n_partition=int(sys.argv[3])

data_RDD=sc.textFile(input_file)
data=data_RDD.map(json.loads)

# Default Partition

t1=time.time()
interim_data=data.map(lambda x: [x['business_id'],1])
grp_val=interim_data).groupByKey()
rdd_freq=grp_val.mapValues(sum)
data_tup=rdd_freq.sortBy(lambda x:[-x[1],x[0]])
task6=data_tup.map(lambda x:list(x)).take(10)
t2=time.time()

def_part_time=t2-t1
#calculating number of partitions for default partiton
def_part_cnt=interim_data.getNumPartitions()

# Calculating number of items per partition

def counter(indx, rdd):
    count = 0
    for i in rdd:
        count += 1
    yield count

def_num_per_part=interim_data.mapPartitionsWithIndex(counter).collect()

# Creating custom Partition function
# Using hash to divide as equal as possible
def custom_part(id):
    return hash(id)

review_custom = interim_data.partitionBy(n_partition, custom_part)
t1=time.time()
grp_val=review_custom .groupByKey()
rdd_freq=grp_val.mapValues(sum)
data_tup=rdd_freq.sortBy(lambda x:[-x[1],x[0]])
task6=data_tup.map(lambda x:list(x)).take(10)

t2=time.time()
n_part_time=t2-t1

#calculating number of partitions for customised partiton
cus_part_cnt=review_custom.getNumPartitions()

# Calculating number of items per partition in customised ppartition function

cus_num_per_part=review_custom.mapPartitionsWithIndex(counter).collect()

json_dict=dict()
json_dict['default']=dict()
json_dict['customized']=dict()


json_dict['default']['n_partition']=def_part_cnt
json_dict['default']['n_items']=def_num_per_part
json_dict['default']['exe_time']=def_part_time

json_dict['customized']['n_partition']=cus_part_cnt
json_dict['customized']['n_items']=cus_num_per_part
json_dict['customized']['exe_time']=n_part_time

final_json=json.dumps(json_dict)

with open(output_file_path, "w") as outfile:
    json.dump(json_dict, outfile)

