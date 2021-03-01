import task1
from task1 import Rdd
import findspark
import pyspark
import math
from pyspark import SparkContext
from graphframes import *
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as func
#from pyspark.sql.functions import col, lit, when
# Making a graph
# Nodes = Users
# Edges = comments
#sc = SparkContext.getOrCreate()
# #sqlContext = SQLContext(sc)
#nodes = sqlContext.createDataFrame('data/users.csv')
#edges = sqlContext.createDataFrame('data/')


def graph_of_posts_comments(postsrdd, commentsrdd, usersrdd):

    nodes = usersrdd
    posts = postsrdd
    comments = commentsrdd
    #graph = posts.id

    # ------------ Joining the two RDDs ----------------
    joined_comments_and_posts = comments.join(posts).map(
        lambda x: (x[1], 1)).reduceByKey(lambda a, b: a+b)
    joined_comments_and_posts = joined_comments_and_posts.map(
        lambda x: (x[0][0], x[0][1], x[1]))
    joined_comments_and_posts.show()
    return joined_comments_and_posts


def graphToDataframe(joined_RDD):
    #df = joined_RDD.toDF()
    # df.show()
    return


def usersWithMostComments(DataFrame):
    top10 = DataFrame.groupBy('_1').sum('_3').sort(
        func.Column("sum(_3)").desc().take(10))
    return top10


if __name__ == '__main__':
    sc = SparkContext.getOrCreate()
    spark = SparkSession(sc)
    rdd = Rdd()
    rdd.returnRddClass()
    joined_RDD = graph_of_posts_comments(
        rdd.getPosts(), rdd.getComments(), rdd.getusers())
    rdd_data_frame = graphToDataframe(joined_RDD)
    usersWithMostComments(rdd_data_frame)
