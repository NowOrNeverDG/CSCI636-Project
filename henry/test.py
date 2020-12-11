import findspark
findspark.init("/opt/spark")
from pyspark import SparkConf, SparkContext

ABSOLUTE_DATA_PATH = "/home/hui/Projects/DTSC701-project/dataset/ml-100k/"

def loadMovieNames():
    """
    load another dataset
    :return: name of the movies (dict)
    """
    movieNames = {}
    # encoding with UTF-8, otherwise encoding issue occur
    with open(ABSOLUTE_DATA_PATH + "u.item", encoding='utf-8', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf=conf)

# broadcast to every node in the cluster so that it can be used whenever needed
nameDict = sc.broadcast(loadMovieNames())

lines = sc.textFile("file:///" + ABSOLUTE_DATA_PATH + "u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x + y)

flipped = movieCounts.map(lambda x: (x[1], x[0]))
sortedMovies = flipped.sortByKey()

# mapping the movie name base on movie ID
sortedMoviesWithNames = sortedMovies.map(lambda countMovie: (nameDict.value[countMovie[1]], countMovie[0]))

results = sortedMoviesWithNames.collect()

for result in results:
    print(result)