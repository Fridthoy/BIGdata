import task1
from task1 import Rdd
import findspark
import pyspark
from pyspark import SparkContext
from graphframes import *
from pyspark.sql import SparkSession, SQLContext
#from pyspark.sql.functions import col, lit, when

# Making a graph
# Nodes = Users
# Edges = comments
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
print(Rdd)
nodes = sqlContext.createDataFrame([])
edges = sqlContext.createDataFrame([])

print(nodes)


def graphing():

    return
