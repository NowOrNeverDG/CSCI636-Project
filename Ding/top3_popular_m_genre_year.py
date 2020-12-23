# Find out the top ten popular movies ratings of all time

# u.data include:four column
# user_id, item_id, rating, timestamp
# item_id is the column the parse machine needs evaluated.
import itertools
from pyspark.shell import spark
import dateutil.parser as parser
import datetime

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
listx = list(map(int, arr))
print(listx)

# 4 switch new array to Rdd map
rdd = spark.sparkContext.parallelize(listx)

# 5.re-map and reduce and sort
rdd2 = rdd.map(lambda word: (word, 1))
rdd3 = rdd2.reduceByKey(lambda a, b: a + b)

rdd4 = rdd3.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)  # [(count, ID)]

print("rdd = ", rdd4.collect())

genre_info = []
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
            temp_temp = [int(x) for x in fields[5:(len(fields))]]
            year_str = fields[2]
            # print("年份: ", year_str, " ID: ", fields[0])
            if year_str != "":
                year_int = parser.parse(year_str).year
            else:
                year_int = 0
            temp_temp.insert(0, year_int)
            movie_names[int(fields[0])] = temp_temp

            ########
            del fields[0: 5]
            genre_info.extend(fields)
            ########
    print("movie_names = ", movie_names)
    numbers = [int(x) for x in genre_info]
    print("genre_info = ", numbers)
    return movie_names


# 7 iterate most popular genre
genreDict = loadMovieGenre()

# 一个有19个key的空字典
pop_m_in_genre_dic = {}
for num in range(1,20):
    pop_m_in_genre_dic[num] = []
print(pop_m_in_genre_dic)

# 一个有100个key的空字典
pop_m_by_year_dic = {}
for num in range(1900, 2020):
    pop_m_by_year_dic[num] = []
print(pop_m_by_year_dic)
# 8
def multip_count(genre_v):
    print("genre_v = ", genre_v)
# list(map(multip_count, genre_v))

# METHOD 1:
for i in range(len(rdd4.collect())):
    kv_pair = rdd4.collect()[i] # (count,id)
    m_count = kv_pair[0]
    m_id = kv_pair[1]
    m_genre = genreDict[kv_pair[1]]
    num = 0
    m_year = m_genre[0]
    if m_year in pop_m_by_year_dic.keys():
        pop_m_by_year_dic[m_year].append(m_id)
print(pop_m_by_year_dic)

# METHOD 2:
for i in range (len(rdd4.collect())):
    kv_pair = rdd4.collect()[i]  # (count,id)
    m_count = kv_pair[0]
    m_id = kv_pair[1]
    m_genre = genreDict[kv_pair[1]]
    for i2 in range(1900, 2020):
        if i2 == m_genre[0]:
            pop_m_by_year_dic[i2].append(m_id)

print(pop_m_by_year_dic)



['Star Wars (1977)', 'Contact (1997)', 'Fargo (1996)', 'Return of the Jedi (1983)', 'Liar Liar (1997)', 'English Patient, The (1996)', 'Scream (1996)', 'Toy Story (1995)', 'Air Force One (1997)']










