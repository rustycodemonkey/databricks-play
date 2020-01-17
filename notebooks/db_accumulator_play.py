# Databricks notebook source
print(type(sc))

# COMMAND ----------

num = sc.accumulator(0)


def f(x):
    # global num
    num.add(x)


rdd = sc.parallelize([1, 2, 3, 4]).foreach(f)
print("Accumulated value is {}".format(num.value))


# COMMAND ----------


# The characters we wish to find the degree of separation between:
# SpiderMan
startCharacterID = 5306
# ADAM 3,031 (who?)
targetCharacterID = 14

# Our accumulator, used to signal when we find the target character during
# our BFS traversal.
hitCounter = sc.accumulator(0)


def convertToBFS(line):
    fields = line.split()
    heroID = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))

    # White = Unprocessed
    color = 'WHITE'
    # 9999 is like infinite distance
    distance = 9999

    if heroID == startCharacterID:
        # Gray = Needs to be expanded
        color = 'GRAY'
        distance = 0
    # (1234, ([1, 2, 3, 4, 5], 9999, WHITE))
    return heroID, (connections, distance, color)


def createStartingRdd():
    inputFile = sc.textFile("s3a://mypersonaldumpingground/spark_taming_data/Marvel-Graph.txt")
    return inputFile.map(convertToBFS)


def bfsMap(node):
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    # If this node needs to be expanded...
    if color == 'GRAY':
        for connection in connections:
            newCharacterID = connection
            newDistance = distance + 1
            newColor = 'GRAY'
            if targetCharacterID == connection:
                hitCounter.add(1)

            newEntry = (newCharacterID, ([], newDistance, newColor))
            results.append(newEntry)

        # We've processed this node, so color it black
        color = 'BLACK'

    # Emit the input node so we don't lose it.
    results.append((characterID, (connections, distance, color)))
    return results


def bfsReduce(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    distance = 9999
    color = color1
    edges = []

    # See if one is the original node with its connections.
    # If so preserve them.
    if len(edges1) > 0:
        edges.extend(edges1)
    if len(edges2) > 0:
        edges.extend(edges2)

    # Preserve minimum distance
    if distance1 < distance:
        distance = distance1

    if distance2 < distance:
        distance = distance2

    # Preserve darkest color
    if color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK'):
        color = color2

    if color1 == 'GRAY' and color2 == 'BLACK':
        color = color2

    if color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK'):
        color = color1

    if color2 == 'GRAY' and color1 == 'BLACK':
        color = color1

    return edges, distance, color


#Main program here:
iterationRdd = createStartingRdd()
# print(iterationRdd.take(1))

for iteration in range(0, 10):
    print("Running BFS iteration# " + str(iteration+1))

    # Create new vertices as needed to darken or reduce distances in the
    # reduce stage. If we encounter the node we're looking for as a GRAY
    # node, increment our accumulator to signal that we're done.
    mapped = iterationRdd.flatMap(bfsMap)

    # Note that mapped.count() action here forces the RDD to be evaluated, and
    # that's the only reason our accumulator is actually updated.
    print("Processing " + str(mapped.count()) + " values.")

    # print(hitCounter.value)
    if hitCounter.value > 0:
        print("Hit the target character! From " + str(hitCounter.value) + " different direction(s).")
        break

    # Reducer combines data for each character ID, preserving the darkest
    # color and shortest path.
    iterationRdd = mapped.reduceByKey(bfsReduce)



### Original Example ###
#Boilerplate stuff:
# from pyspark import SparkConf, SparkContext
#
# conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
# sc = SparkContext(conf = conf)
#
# # The characters we wish to find the degree of separation between:
# startCharacterID = 5306 #SpiderMan
# targetCharacterID = 14  #ADAM 3,031 (who?)
#
# # Our accumulator, used to signal when we find the target character during
# # our BFS traversal.
# hitCounter = sc.accumulator(0)
#
# def convertToBFS(line):
#     fields = line.split()
#     heroID = int(fields[0])
#     connections = []
#     for connection in fields[1:]:
#         connections.append(int(connection))
#
#     color = 'WHITE'
#     distance = 9999
#
#     if (heroID == startCharacterID):
#         color = 'GRAY'
#         distance = 0
#
#     return (heroID, (connections, distance, color))
#
#
# def createStartingRdd():
#     inputFile = sc.textFile("file:///sparkcourse/marvel-graph.txt")
#     return inputFile.map(convertToBFS)
#
# def bfsMap(node):
#     characterID = node[0]
#     data = node[1]
#     connections = data[0]
#     distance = data[1]
#     color = data[2]
#
#     results = []
#
#     #If this node needs to be expanded...
#     if (color == 'GRAY'):
#         for connection in connections:
#             newCharacterID = connection
#             newDistance = distance + 1
#             newColor = 'GRAY'
#             if (targetCharacterID == connection):
#                 hitCounter.add(1)
#
#             newEntry = (newCharacterID, ([], newDistance, newColor))
#             results.append(newEntry)
#
#         #We've processed this node, so color it black
#         color = 'BLACK'
#
#     #Emit the input node so we don't lose it.
#     results.append( (characterID, (connections, distance, color)) )
#     return results
#
# def bfsReduce(data1, data2):
#     edges1 = data1[0]
#     edges2 = data2[0]
#     distance1 = data1[1]
#     distance2 = data2[1]
#     color1 = data1[2]
#     color2 = data2[2]
#
#     distance = 9999
#     color = color1
#     edges = []
#
#     # See if one is the original node with its connections.
#     # If so preserve them.
#     if (len(edges1) > 0):
#         edges.extend(edges1)
#     if (len(edges2) > 0):
#         edges.extend(edges2)
#
#     # Preserve minimum distance
#     if (distance1 < distance):
#         distance = distance1
#
#     if (distance2 < distance):
#         distance = distance2
#
#     # Preserve darkest color
#     if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
#         color = color2
#
#     if (color1 == 'GRAY' and color2 == 'BLACK'):
#         color = color2
#
#     if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
#         color = color1
#
#     if (color2 == 'GRAY' and color1 == 'BLACK'):
#         color = color1
#
#     return (edges, distance, color)
#
#
# #Main program here:
# iterationRdd = createStartingRdd()
#
# for iteration in range(0, 10):
#     print("Running BFS iteration# " + str(iteration+1))
#
#     # Create new vertices as needed to darken or reduce distances in the
#     # reduce stage. If we encounter the node we're looking for as a GRAY
#     # node, increment our accumulator to signal that we're done.
#     mapped = iterationRdd.flatMap(bfsMap)
#
#     # Note that mapped.count() action here forces the RDD to be evaluated, and
#     # that's the only reason our accumulator is actually updated.
#     print("Processing " + str(mapped.count()) + " values.")
#
#     if (hitCounter.value > 0):
#         print("Hit the target character! From " + str(hitCounter.value) \
#             + " different direction(s).")
#         break
#
#     # Reducer combines data for each character ID, preserving the darkest
#     # color and shortest path.
#     iterationRdd = mapped.reduceByKey(bfsReduce)


# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

dbutils.fs.rm("/movie-sims", recurse=True)