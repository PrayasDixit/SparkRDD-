from pyspark.context import SparkContext
import json
import datetime as datetime
import time
import sys

sc = SparkContext.getOrCreate()

review_file_path = sys.argv[1]
business_file_path = sys.argv[2]
output1_file_path = sys.argv[3]
output2_file_path = sys.argv[4]

test_review = sc.textFile(review_file_path).map(json.loads)
data_business = sc.textFile(business_file_path).map(json.loads)

# For Spark output and time

x = data_business.map(lambda x: (x['business_id'], x['city']))
y = test_review.map(lambda x: (x['business_id'], x['stars']))

res = x.leftOuterJoin(y)
res1 = res.filter(lambda x: x[1][1] != None)
res2 = res1.map(lambda x: list(x[1]))
res2 = res2.groupByKey().mapValues(lambda x: sum(x) / len(x))

t1 = time.time()

res2 = res2.sortBy(lambda x: [-x[1], x[0]])
res2 = res2.map(lambda x: list(x)).take(10)
t2 = time.time()
time_spark = t2 - t1

with open(output1_file_path, 'w') as t3:
    t3.write("city,star\n")
    for i in res2:
        t3.write(str(i[0]) + ',' + str(i[1]) + '\n')

# For Python output and time

x = data_business.map(lambda x: (x['business_id'], x['city']))
y = test_review.map(lambda x: (x['business_id'], x['stars']))
res = x.leftOuterJoin(y)
res1 = res.filter(lambda x: x[1][1] != None)
res2 = res1.map(lambda x: list(x[1]))
res2 = res2.groupByKey().mapValues(lambda x: sum(x) / len(x))

res2 = res2.collect()
t1 = time.time()
res2.sort(key=lambda x: (-x[1], x[0]))
res2[0:10]
t2 = time.time()
time_py = t2 - t1

reason = "Python works much faster because the result is already calculated and stored at single place and shuffle is not happening whereas in Spark case shuffle is happening therefore it will take more time"

json_dict = dict()
json_dict['m1'] = time_py
json_dict['m2'] = time_spark
json_dict['reason'] = reason

final_json = json.dumps(json_dict)

with open(output2_file_path, "w") as outfile:
    json.dump(json_dict, outfile)