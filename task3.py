import task1
from task1 import Rdd
import findspark
import pyspark
import math
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as func
from pyspark.sql.window import Window


# Making a graph
# Nodes = Users
# Edges = comments
#sc = SparkContext.getOrCreate()
# #sqlContext = SQLContext(sc)
#nodes = sqlContext.createDataFrame('data/users.csv')
#edges = sqlContext.createDataFrame('data/')


def graph_of_posts_comments(postsrdd, commentsrdd):
    posts = postsrdd
    comments = commentsrdd

    # ------------ Joining the two RDDs ----------------
    joined_comments_and_posts = comments.join(posts).map(
        lambda columns: (columns[1], 1)).reduceByKey(lambda commentid, postid: commentid+postid)
    joined_comments_and_posts = joined_comments_and_posts.map(
        lambda columns: (columns[0][0], columns[0][1], columns[1]))

    print(joined_comments_and_posts.take(5))
    return joined_comments_and_posts


def graphComment2(postrdd, commentrdd):
    posts = postrdd
    comments = commentrdd

    postHeader = posts.first()
    commentHeader = comments.first()

    myposts = posts.filter(lambda x: x != postHeader).map(
        lambda x: (x[0], x[6])).filter(lambda x: x[1] != ("-1" and "NULL"))

    myComments = comments.filter(lambda x: x != commentHeader).map(
        lambda x: (x[0], x[4])).filter(lambda x: x[1] != ("-1" and "NULL"))

    joined = myComments.join(myposts)

    joined = joined.map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x+y)

    joined = joined.map(lambda x: (x[0][0], x[0][1], x[1]))

    return joined


def graphToDataframe(joined_RDD):
    df = joined_RDD.toDF()
    return df


def findMostComments(df):
    df.show()
    newdf = df.drop('_2')
    newdf = newdf.withColumnRenamed('_1', 'ID of comment owner')
    newdf = newdf.withColumnRenamed('_3', 'Number of comments')
    newdf = newdf.groupBy('ID of comment owner').sum('Number of comments')
    newdf = newdf.orderBy("sum(Number of comments)", ascending=False)
    newdf.show(10)
    return newdf


def generateNames(userRdd, commentDf):

    # -------- Initializing the userRDD and converting to DF -----------
    header = userRdd.first()
    user = userRdd.filter(lambda x: x != header).map(lambda x: (x[0], x[3]))
    df = user.toDF()
    df.show(10)

    # ---- Joing on userID ----
    df = df.withColumnRenamed('_1', 'ID of comment owner')
    df = df.withColumnRenamed('_2', 'Dislayed name')
    newdf = commentDf.join(df, on=['ID of comment owner'])
    # ------ Sorting on number of comments ---------
    newdf = newdf.orderBy("sum(Number of comments)", ascending=False)
    newdf.show(10)

    return newdf


def saveDF(dataInput):
    dataInput.write.option("header", True).csv("Save2.csv")
    return


if __name__ == '__main__':

    rdd = Rdd()
    rdd.returnRddClass()

    graphComment2(rdd.getPosts(), rdd.getComments())

    joined_RDD = graphComment2(
        rdd.getPosts(), rdd.getComments())

    rdd_data_frame = graphToDataframe(joined_RDD)

    mostComments = findMostComments(rdd_data_frame)

    generateNames(rdd.getusers(), mostComments)
