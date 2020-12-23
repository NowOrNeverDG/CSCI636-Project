# Find out the top ten popular movies ratings of all time

# u.data include:four column
# user_id, item_id, rating, timestamp
# item_id is the column the parse machine needs evaluated.
import itertools
import time
from pyspark.shell import spark

print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

# 1.got path of u.data
rdd = spark.sparkContext.textFile("/Users/ding/Downloads/ml-100k/u5.test.test")

# 2.split data by tab
rdd2 = rdd.flatMap(lambda x: x.split("\t"))
print(rdd2.collect())
# 3.extract the column needed to new array
arr = []
i = 1
for i in range(1, len(rdd2.collect()), 4):
    arr.append(rdd2.collect()[i])
listx = list(map(int, arr))

# 4 switch new array to Rdd map
rdd = spark.sparkContext.parallelize(listx)

# 5.re-map and reduce and sort
rdd2 = rdd.map(lambda word: (word, 1))
rdd3 = rdd2.reduceByKey(lambda a, b: a + b)

rdd4 = rdd3.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)
print("rdd = ", rdd4.collect())  # [(count, ID)]

genre_info = []
m_name = {}


# 6.get genre data from u.item
def loadMovieGenre():
    # read the u.item since it requires special encoding
    # therefore, context reader does not work in this case
    # OUTPUT: (key: movie_id, val: {genre collection})
    movie_names = {}
    with open("/Users/ding/Downloads/ml-100k/u.item", encoding='utf-8', errors='ignore') as reader:
        for line in reader:
            fields = line.split('|')
            temp = fields[-1:][0].strip()
            fields[-1:] = temp
            ########
            # movie_names[int(fields[0])] = fields[5:(len(fields))]
            ########
            temp_temp = [int(x) for x in fields[5:(len(fields))]]
            movie_names[int(fields[0])] = temp_temp
            m_name[int(fields[0])] = fields[1]
            ########
            # del fields[0: 5]
            # genre_info.extend(fields)
            ########
    print("movie_names = ", movie_names)
    # numbers = [int(x) for x in genre_info]
    # print("genre_info = ", numbers)
    print("M_name = ", m_name)
    return movie_names


# 7 iterate most popular genre
genreDict = loadMovieGenre()
genre_count = [0 for i in range(19)]  # Initialize the arr to contain result

# METHOD:1
movie_count = 0
movie_point = 0


def classify_genre(gd_value):
    global movie_point
    global movie_count
    movie_point = movie_point + 1
    i = 0
    for i in range(len(rdd4.collect())):
        kv_pair = rdd4.collect()[i]

        if movie_point == kv_pair[1]:
            movie_count = kv_pair[0]
            break
    i = 0
    for i in range(19):
        genre_count[i] = genre_count[i] + gd_value[i] * movie_count
    movie_count = 0


genre_dict_values = genreDict.values()
list(map(classify_genre, genre_dict_values))
print(genre_count)

##########
# METHOD: 2
# i = 0
# for i in range(len(rdd3.collect())):
#     kv_pair = rdd3.collect()[i]
#     cur_val = genreDict[kv_pair[0]]  # rdd:(count,ID)
#     print(cur_val)
#
#     j = 0
#     for j in range(len(cur_val)):
#             genre_count[j] = genre_count[j] + cur_val[j] * kv_pair[1]
#
# print(genre_count)
##########
print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

# [310, 25589, 13753, 3605, 7182, 29832, 8055, 758, 39895, 1352, 1733, 5317, 4954, 5245, 19461, 12730, 21872, 9398,
# 1854]
# ['Drama', 'Comedy', 'Action', 'Thriller', 'Romance', 'Adventure', 'Sci-Fi', 'War', 'Crime', 'Children']
