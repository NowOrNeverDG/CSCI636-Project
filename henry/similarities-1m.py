# please note, when run this script, please add the movie ID at the end of the cmd
# so that the program knows which movie you talking about
import sys
import findspark
findspark.init("/opt/spark")
from pyspark import SparkConf, SparkContext
from math import sqrt

ABSOLUTE_DATA_PATH = "home/hui/Projects/DTSC701-project/dataset/ml-100k/"


def loadMovieNames():
    """
    FUNC: read every line of u.item and then return title and year as a dictionary
    OUTPUT: dictionary -> movie name and publish year
    """
    movieNames = {}
    with open(ABSOLUTE_DATA_PATH + "movies.dat", encoding='ascii', errors='ignore') as f:
        for line in f:
            fields = line.split('::')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


# Python 3 doesn't let you pass around unpacked tuples,
# so we explicitly extract the ratings now.
def makePairs(userRatings):
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return ((movie1, movie2), (rating1, rating2))


def filterDuplicates(userRatings):
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return movie1 < movie2


def computeCosineSimilarity(ratingPairs):
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))

    return (score, numPairs)


# [*] will make use of all cores in the computer to run the program
conf = SparkConf().setMaster("local[*]").setAppName("MovieSimilarities")
sc = SparkContext(conf=conf)

print("\nLoading movie names...")
nameDict = loadMovieNames()

data = sc.textFile("file:///" + ABSOLUTE_DATA_PATH + "ratings.dat")

# Map ratings to key / value pairs: user ID => movie ID, rating
ratings = data.map(lambda l: l.split("::")).map(
    lambda l: (int(l[0]), (int(l[1]), float(l[2]))))
# e.g. (user ID, (movie ID, rating))

# Emit every movie rated together by the same user.
# Self-join to find every combination.
joinedRatings = ratings.join(ratings)

# At this point our RDD consists of userID => ((movieID, rating), (movieID, rating))

# Filter out duplicate pairs
uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

# Now key by (movie1, movie2) pairs.
moviePairs = uniqueJoinedRatings.map(makePairs)

# We now have (movie1, movie2) => (rating1, rating2)
# Now collect all ratings for each movie pair and compute similarity
moviePairRatings = moviePairs.groupByKey()

# We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
# Can now compute similarities.
moviePairSimilarities = moviePairRatings.mapValues(
    computeCosineSimilarity).cache()
# Note: we cache in memory. Use persist() will store the data in disk rather than just the memory
# Note: multiple cores will produce multiple results/ txt in persist() if we wanna store the data on disk

# Save the results if desired
# moviePairSimilarities.sortByKey()
# moviePairSimilarities.saveAsTextFile("movie-sims")

# Extract similarities for the movie we care about that are "good".
if (len(sys.argv) > 1):
    # make sure there's an input from the user

    # result score
    score_Threshold = 0.97
    # number of ppl who watched the same movie
    coOccurence_Threshold = 50

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter(lambda pairSim:
                                                   (pairSim[0][0] ==
                                                    movieID or pairSim[0][1] == movieID)
                                                   and pairSim[1][0] > score_Threshold and pairSim[1][
                                                       1] > coOccurence_Threshold)

    # Sort by quality score.
    # flight the key and val, then sort in descending order and then take the top 10
    results = filteredResults.map(lambda pairSim: (
        pairSim[1], pairSim[0])).sortByKey(ascending=False).take(10)

    print("Top 10 similar movies for " + nameDict[movieID])
    for result in results:
        (sim, pair) = result
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = pair[0]
        if (similarMovieID == movieID):
            similarMovieID = pair[1]
        print(nameDict[similarMovieID] + "\tscore: " +
              str(sim[0]) + "\tstrength: " + str(sim[1]))
