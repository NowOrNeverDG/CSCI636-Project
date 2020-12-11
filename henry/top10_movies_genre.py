# find the top 10 movies in each genre based on
# their movie ratings based on user rating
import re
import collections

# config spark location before importing pyspark
# otherwise it will cause an error in Linux
import findspark
findspark.init("/opt/spark")
from pyspark import SparkConf, SparkContext


ABSOLUTE_DATA_PATH = "home/hui/Projects/DTSC701-project/dataset/ml-100k/"

MOVIE_GENRE = {"unknown": 0, "Action": 0, "Adventure": 0, "Animation": 0, "Children's": 0,
                      "Comedy": 0, "Crime": 0, "Documentary": 0, "Drama": 0, "Fantasy": 0, "Film-Noir": 0, "Horror": 0,
                      "Musical": 0, "Mystery": 0, "Romance": 0, "Sci-Fi": 0, "Thriller": 0, "War": 0, "Western": 0}

GENRE_LIST = list(MOVIE_GENRE)

# set up RDD locally
conf = SparkConf().setMaster("local").setAppName("top10Genre")
sc = SparkContext(conf=conf)


# set up RDD remotely
# MASTER_NAME = "spark://china-home:7077"
# PROJECT_NAME = "top10Genre"
# conf = SparkConf().setMaster(MASTER_NAME).setAppName(PROJECT_NAME)
# sc = SparkContext(conf=conf)


def parseRatings(line):
    # get get the movie id and ratings from the dataset
    # INPUT: a single line of dataset
    # OUTPUT: (key: movie_id, val: ratings)
    fields = line.split()
    return (int(fields[1]), int(fields[2]))


def loadMovieGenre():
  # read the u.item since it requires special encoding
  # therefore, context reader does not work in this case
  # OUTPUT: (key: movie_id, val: (title, {genre collection list}))
    movieNames = {}
    with open("/" + ABSOLUTE_DATA_PATH + "u.item", encoding='utf-8', errors='ignore') as reader:
        for line in reader:
            fields = line.split('|')
            
            # trim the additional "\n" in the last element
            temp = fields[-1:][0].strip()
            fields[-1:] = temp

            movieNames[int(fields[0])] = (fields[1], fields[5:(len(fields))])
        
        reader.close()
    return movieNames


def merge_genreDict():
    # merge MOVIE_GENRE with genreDict to form a new dict
    # OUTPUT: (movie_id, (title, ["x", "y", "z"]))
    
    merge = {}
    # access genre list
    global GENRE_LIST
    
    for key, val in genreDict.items():
        temp = []
        counter = 0
        for ele in val[1]:
            if str(ele) == "1":
                temp.append(GENRE_LIST[counter])

            counter = counter + 1
        
        merge[key] = (val[0], temp)
    
    return merge


def merge_rating_genre():
    # merge genre_ratings_flipped with MOVIE_GENRE
    # OUTPUT: merged_rdd --> ("genre type", [(title, rating sum), ... (n, n)])
    result = {}
    global GENRE_LIST
    global MOVIE_GENRE
    global genre_ratings_flipped

    for item in GENRE_LIST:
        a = []
        for key, val in genre_ratings_flipped.collect():
        # make sure we don't exceed 10
            # check genre exists in unique genre list of genre_ratings_flipped
            if MOVIE_GENRE[item] < 10 and item in key[1]:
                a.append((key[0], val))
                # increment the genre counting
                MOVIE_GENRE[item] += 1
        
            result[item] = (a)
    
    return result


# read the rating data
ratings = sc.textFile("file:///" + ABSOLUTE_DATA_PATH + "u.data")
ratings_parse = ratings.map(parseRatings)

# sum rating
ratings_count = ratings_parse.reduceByKey(lambda x, y: x + y)

# swip kay and val AND THEN sort them in descending order
ratings_sorted = ratings_count.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)

# load the movie genre
genreDict = loadMovieGenre()

# merge the two RDDs
# broadcast to every node in the cluster so that it can be used whenever needed
genreDict_merged = sc.broadcast(merge_genreDict())
# genreDict_merged = merge_genreDict()

# we are able to use "value" func since the RDD is broadcast across all nodes in the cluster
genre_ratings_flipped = ratings_sorted.map(lambda x: ((genreDict_merged.value[x[1]]), (x[0])))
# now the RDD is in => ((title, ['x', 'y', 'z']), rating_sum)

genre_ratings = merge_rating_genre()

for key, val in genre_ratings.items():
    print('%s, %s \n' % (key, val))
