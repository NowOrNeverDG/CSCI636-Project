# Find out the top ten popular movies ratings of all time

# u.data include:four column
# user_id, item_id, rating, timestamp
# item_id is the column the parse machine needs evaluated.

from pyspark.shell import spark

# 1.got path of u.data
rdd = spark.sparkContext.textFile("/Users/ding/Downloads/ml-100k/test_u2.test")

# 2.split data by tab
rdd2 = rdd.flatMap(lambda x: x.split("\t"))
print(rdd2.collect())

# 3.extract the column needed to new array
arr = []
i = 1
for i in range(1, len(rdd2.collect()), 4):
    arr.append(rdd2.collect()[i])
list = list(map(int, arr))
print(list)

# 4 switch new array to Rdd map
rdd = spark.sparkContext.parallelize(list)

# 5.re-map and reduce and sort
rdd2 = rdd.map(lambda word: (word, 1))
rdd3 = rdd2.reduceByKey(lambda a, b: a + b)

rdd4 = rdd3.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)
print("rdd = ", rdd4.collect())  # [(count, ID)]
