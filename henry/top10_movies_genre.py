# find the top 10 movies in each genre based on
# their movie ratings based on user rating

from pyspark import SparkConf, SparkContext
import re
import collections
# import findspark
# findspark.init("/opt/spark")


ABSOLUTE_DATA_PATH = "home/hui/Projects/DTSC701-project/dataset/ml-100k/"

# set up RDD locally
conf = SparkConf().setMaster("local").setAppName("top10Genre")
sc = SparkContext(conf=conf)


# set up RDD remotely
# MASTER_NAME = "spark://china-home:7077"
# PROJECT_NAME = "top10Genre"
# conf = SparkConf().setMaster(MASTER_NAME).setAppName(PROJECT_NAME)
# sc = SparkContext(conf=conf)


def parseRatings(line):
    fields = line.split()
    return (int(fields[1]), int(fields[2]))


def loadMovieGenre():
  # read the u.item since it requires special encoding
  # therefore, context reader does not work in this case
  # OUTPUT: (key: movie_id, val: {genre collection})
    movieNames = {}
    with open("/" + ABSOLUTE_DATA_PATH + "u.item", encoding='ascii', errors='ignore') as reader:
        for line in reader:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[5:(len(fields) - 1)]
    return movieNames


# read the rating data
ratings = sc.textFile("file:///" + ABSOLUTE_DATA_PATH + "u.data")
ratings_parse = ratings.map(parseRatings)

# read the movie genre
genreDict = loadMovieGenre()

# for key, val in ratings_parse.collect():
#   print(str(key) + ", " + str(val))

# for key, val in genreDict.items():
#     print('%s, %s' % (key, val))

