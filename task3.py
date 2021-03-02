import task1
from task1 import Rdd
import pyspark
import math
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as func
from pyspark.sql.window import Window


# Making a graph
# Nodes = Users
# Edges = comments
# sc = SparkContext.getOrCreate()
# #sqlContext = SQLContext(sc)
# nodes = sqlContext.createDataFrame('data/users.csv')
# edges = sqlContext.createDataFrame('data/')

def graphComment2(postrdd, commentrdd):
    posts = postrdd
    comments = commentrdd

    postHeader = posts.first()
    commentHeader = comments.first()

    # ------- Extracting the columns we need -----------
    myposts = posts.filter(lambda x: x != postHeader).map(
        lambda x: (x[0], x[6]))
    myComments = comments.filter(lambda x: x != commentHeader).map(
        lambda x: (x[0], x[4]))

    # ------- Preprocessing the id-columns -----------
    myposts = myposts.filter(lambda x: x[1] != ("-1" and "NULL"))

    myComments = myComments.filter(lambda x: x[1] != ("-1" and "NULL"))

    # -------- Joining the two RDDs ----------
    graph = myComments.join(myposts)
    # ------------- Counting the number of instances -------------
    graph = graph.map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x+y)
    # print(graph.take(10))
    graph = graph.map(lambda x: (x[0][0], x[0][1], x[1]))
    # print(graph.take(10))
    return graph


def graphToDataframe(joined_RDD):
    print("--------- Task 3.2 ---------------")
    df = joined_RDD.toDF()
    print("---- Succsessfully made RDD into Dataframe ------- ")
    return df


def findMostComments(df):
    print("--------- Task 3.3 ---------------")
    # df.show()
    print(" ")
    print("# -------- finding the top 10 users who have written the most comments ----------")

    newdf = df
    newdf = newdf.withColumnRenamed('_1', 'ID of comment owner')
    newdf = newdf.withColumnRenamed('_3', 'Number of comments')
    newdf = newdf.groupBy('ID of comment owner').sum('Number of comments')
    newdf = newdf.orderBy("sum(Number of comments)", ascending=False)
    newdf.show(10)
    print("---------------------------------------------------------------------------------")
    # -------- finding the top 10 users with most comments on their post ----------
    print(" ")
    # print("# -------- finding the top 10 users with most comments on their post ----------")

    top10users = df
    top10users = top10users.withColumnRenamed('_1', 'ID of comment owner')
    top10users = top10users.withColumnRenamed('_2', 'ID of post owner')
    top10users = top10users.withColumnRenamed('_3', 'Number of comments')
    top10users = top10users.groupBy(
        'ID of post owner').sum('Number of comments')

    # top10users.show()
    print(" ")
    print("---------------------------------------------------------------------------------")
    return newdf, top10users


def generateNames(userRdd, top10posts, df):
    print("--------- Task 3.4 ---------------")

    # -------- Initializing the userRDD and converting to DF -----------
    header = userRdd.first()
    user = userRdd.filter(lambda x: x != header).map(lambda x: (x[0], x[3]))
    df = user.toDF()
    # df.show(10)

    # ---- Joing on userID ----
    print(" ")
    print("# -------- finding the top 10 users with most comments on their post username ----------")

    df = df.withColumnRenamed('_1', 'ID of post owner')
    df = df.withColumnRenamed('_2', 'Dislayed name')
    newdf = top10posts.join(df, on=['ID of post owner'])
    # ------ Sorting on number of comments ---------
    newdf = newdf.orderBy("sum(Number of comments)", ascending=False)
    newdf.show(10)

    print(" ")
    print("---------------------------------------------------------------------------------")
    return newdf


def saveDF(dataInput):
    print("--------- Task 3.5 ---------------")
    dataInput.write.option("header", True).csv("Save2.csv")
    print("Succsefully made a csv file.")
    return


def main_task3():
    rdd = Rdd()
    rdd.returnRddClass()
    graphComment2(rdd.getPosts(), rdd.getComments())
    joined_RDD = graphComment2(
        rdd.getPosts(), rdd.getComments())
    rdd_data_frame = graphToDataframe(joined_RDD)
    mostComments, top10posts = findMostComments(rdd_data_frame)
    generateNames(rdd.getusers(), top10posts, rdd_data_frame)
    saveDF(rdd_data_frame)
    print(" ")
    print("---------- TASK 3 COMPLETED -----------")
    return


if __name__ == '__main__':
    main_task3()
