from itertools import combinations
from collections import defaultdict
from pyspark import SparkContext
from sys import argv

def minHash(listx, i):
    return min(map(lambda x: (3 * x + 13 * i) % 100, listx))

def jaccardSimilarity(a, b):
    return float(len(set(a) & set(b))) / len(set(a) | set(b))

if __name__ == "__main__":
    sc = SparkContext()
    #outputfile  = "Yiming_Liu_SimilarMovies.txt"
    outputfile = argv[2]
    #file = sc.textFile("ratings.csv")
    file = sc.textFile(argv[1])
    header = file.first()
    data = file.filter(lambda rows: rows != header).map(lambda row: row.split(",")).map(
        lambda x: (int(x[1]), int(x[0])))
    # (movie, [users]) 9066
    movieList = data.groupByKey().mapValues(lambda values: [v for v in values]).sortByKey()

    #print movieList.take(5)

    band1 = movieList.map(lambda x:((minHash(x[1], 0), minHash(x[1], 1), minHash(x[1], 2), minHash(x[1], 3)), x[0]))\
        .groupByKey().map(lambda x:list(x[1])).filter(lambda x: len(x) > 1).flatMap(lambda x: combinations(x, 2))
    band2 = movieList.map(lambda x:((minHash(x[1], 4), minHash(x[1], 5), minHash(x[1], 6), minHash(x[1], 7)), x[0])).groupByKey().map(lambda
     x: list(x[1])).filter(lambda x: len(x) > 1).flatMap(lambda x: combinations(x, 2))
    band3 = movieList.map(lambda x:((minHash(x[1], 8), minHash(x[1], 9), minHash(x[1], 10), minHash(x[1], 11)), x[0])).groupByKey().map(
        lambda x: list(x[1])).filter(lambda x: len(x) > 1).flatMap(lambda x: combinations(x, 2))
    band4 = movieList.map(lambda x:((minHash(x[1], 12), minHash(x[1], 13), minHash(x[1], 14), minHash(x[1], 15)), x[0])).groupByKey().map(
        lambda x: list(x[1])).filter(lambda x: len(x) > 1).flatMap(lambda x: combinations(x, 2))
    band5 = movieList.map(lambda x:((minHash(x[1], 16), minHash(x[1], 17), minHash(x[1], 18), minHash(x[1], 19)), x[0])).groupByKey().map(
        lambda x: list(x[1])).filter(lambda x: len(x) > 1).flatMap(lambda x: combinations(x, 2))
    band6 = movieList.map(lambda x:((minHash(x[1], 20), minHash(x[1], 21), minHash(x[1], 22), minHash(x[1], 23)), x[0])).groupByKey().map(
        lambda x: list(x[1])).filter(lambda x: len(x) > 1).flatMap(lambda x: combinations(x, 2))
    band7 = movieList.map(lambda x:((minHash(x[1], 24), minHash(x[1], 25), minHash(x[1], 26), minHash(x[1], 27)), x[0])).groupByKey().map(
        lambda x: list(x[1])).filter(lambda x: len(x) > 1).flatMap(lambda x: combinations(x, 2))
    band8 = movieList.map(lambda x:((minHash(x[1], 28), minHash(x[1], 29), minHash(x[1], 30), minHash(x[1], 31)), x[0])).groupByKey().map(
        lambda x: list(x[1])).filter(lambda x: len(x) > 1).flatMap(lambda x: combinations(x, 2))
    band9 = movieList.map(lambda x:((minHash(x[1], 32), minHash(x[1], 33), minHash(x[1], 34), minHash(x[1], 35)), x[0])).groupByKey().map(
        lambda x: list(x[1])).filter(lambda x: len(x) > 1).flatMap(lambda x: combinations(x, 2))
    band10 = movieList.map(lambda x:((minHash(x[1], 36), minHash(x[1], 37), minHash(x[1], 38), minHash(x[1], 39)), x[0])).groupByKey().map(
        lambda x: list(x[1])).filter(lambda x: len(x) > 1).flatMap(lambda x: combinations(x, 2))
    band11 = movieList.map(lambda x: ((minHash(x[1], 40), minHash(x[1], 41), minHash(x[1], 42), minHash(x[1], 43)), x[0])) \
        .groupByKey().map(lambda x: list(x[1])).filter(lambda x: len(x) > 1).flatMap(lambda x: combinations(x, 2))
    band12 = movieList.map(lambda x: ((minHash(x[1], 44), minHash(x[1], 45), minHash(x[1], 46), minHash(x[1], 47)), x[0])) \
        .groupByKey().map(lambda x: list(x[1])).filter(lambda x: len(x) > 1).flatMap(lambda x: combinations(x, 2))
    band13 = movieList.map(lambda x: ((minHash(x[1], 48), minHash(x[1], 49), minHash(x[1], 50), minHash(x[1], 51)), x[0])) \
        .groupByKey().map(lambda x: list(x[1])).filter(lambda x: len(x) > 1).flatMap(lambda x: combinations(x, 2))
    band14 = movieList.map(lambda x: ((minHash(x[1], 52), minHash(x[1], 53), minHash(x[1], 54), minHash(x[1], 55)), x[0])) \
        .groupByKey().map(lambda x: list(x[1])).filter(lambda x: len(x) > 1).flatMap(lambda x: combinations(x, 2))
    band15 = movieList.map(lambda x: ((minHash(x[1], 56), minHash(x[1], 57), minHash(x[1], 58), minHash(x[1], 59)), x[0])) \
        .groupByKey().map(lambda x: list(x[1])).filter(lambda x: len(x) > 1).flatMap(lambda x: combinations(x, 2))
    band16 = movieList.map(lambda x: ((minHash(x[1], 60), minHash(x[1], 61), minHash(x[1], 62), minHash(x[1], 63)), x[0])) \
        .groupByKey().map(lambda x: list(x[1])).filter(lambda x: len(x) > 1).flatMap(lambda x: combinations(x, 2))
    band17 = movieList.map(lambda x: ((minHash(x[1], 64), minHash(x[1], 65), minHash(x[1], 66), minHash(x[1], 67)), x[0])) \
        .groupByKey().map(lambda x: list(x[1])).filter(lambda x: len(x) > 1).flatMap(lambda x: combinations(x, 2))
    band18 = movieList.map(lambda x: ((minHash(x[1], 68), minHash(x[1], 69), minHash(x[1], 70), minHash(x[1], 71)), x[0])) \
        .groupByKey().map(lambda x: list(x[1])).filter(lambda x: len(x) > 1).flatMap(lambda x: combinations(x, 2))
    band19 = movieList.map(lambda x: ((minHash(x[1], 72), minHash(x[1], 73), minHash(x[1], 74), minHash(x[1], 75)), x[0])) \
        .groupByKey().map(lambda x: list(x[1])).filter(lambda x: len(x) > 1).flatMap(lambda x: combinations(x, 2))
    band20 = movieList.map(lambda x: ((minHash(x[1], 76), minHash(x[1], 77), minHash(x[1], 78), minHash(x[1], 79)), x[0])) \
        .groupByKey().map(lambda x: list(x[1])).filter(lambda x: len(x) > 1).flatMap(lambda x: combinations(x, 2))
    band21 = movieList.map(lambda x: ((minHash(x[1], 80), minHash(x[1], 81), minHash(x[1], 82), minHash(x[1], 83)), x[0])) \
        .groupByKey().map(lambda x: list(x[1])).filter(lambda x: len(x) > 1).flatMap(lambda x: combinations(x, 2))
    band22 = movieList.map(lambda x: ((minHash(x[1], 84), minHash(x[1], 85), minHash(x[1], 86), minHash(x[1], 87)), x[0])) \
    .groupByKey().map(lambda x: list(x[1])).filter(lambda x: len(x) > 1).flatMap(lambda x: combinations(x, 2))
    band23 = movieList.map(lambda x: ((minHash(x[1], 88), minHash(x[1], 89), minHash(x[1], 90), minHash(x[1], 91)), x[0])) \
        .groupByKey().map(lambda x: list(x[1])).filter(lambda x: len(x) > 1).flatMap(lambda x: combinations(x, 2))
    band24 = movieList.map(lambda x: ((minHash(x[1], 92), minHash(x[1], 93), minHash(x[1], 94), minHash(x[1], 95)), x[0])) \
        .groupByKey().map(lambda x: list(x[1])).filter(lambda x: len(x) > 1).flatMap(lambda x: combinations(x, 2))
    band25 = movieList.map(lambda x: ((minHash(x[1], 96), minHash(x[1], 97), minHash(x[1], 98), minHash(x[1], 99)), x[0])) \
        .groupByKey().map(lambda x: list(x[1])).filter(lambda x: len(x) > 1).flatMap(lambda x: combinations(x, 2))

    calJ = defaultdict(list)
    calJ.update(movieList.collect())
    similarity = band1.union(band2).union(band3).union(band4).union(band5)\
        .union(band6).union(band7).union(band8).union(band9).union(band10)\
        .union(band11).union(band12).union(band13).union(band14).union(band15)\
        .union(band16).union(band17).union(band18).union(band19).union(band20) \
        .union(band21).union(band22).union(band23).union(band24).union(band25) \
        .distinct().map(lambda x: (x, jaccardSimilarity(calJ[x[0]], calJ[x[1]])))


    similarityFilter = similarity.filter(lambda x: x[1] >= 0.5)
    #print similarityFilter.count()
    truthSet = sc.textFile("SimilarMovies.GroundTruth.05.csv").map(lambda row : row.split(",")).map(lambda x: (int(x[0]),int(x[1])))

    resultSet = similarityFilter.sortByKey().map(lambda x:x[0])

    tp = float(resultSet.intersection(truthSet).count())
    #print tp
    fn = float(truthSet.subtract(resultSet).count())
    #print fn
    fp = float(resultSet.subtract(truthSet).count())
    #print fp
    precision = float(tp/(tp + fp))
    print ('precision = ' + str(precision))
    recall = float(tp/(tp + fn))
    print ('recall = ' + str(recall))

    ans = sorted(similarity.collect())
    #print ans.count()
    #print len(ans2)
    #print ans[:5]
    f = open(outputfile, 'w')
    for line in ans:
        f.write(str(line[0][0]) + ',' + str(line[0][1]) + ',' + str(line[1]) + "\n")

    f.close

    sc.stop()
