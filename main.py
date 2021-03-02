from pyspark import SparkConf
from pyspark.sql import SparkSession
import findspark
import task1
import task2
import task3
findspark.init()

spark = SparkSession.builder.master(
    "local").appName("badgesLoad").getOrCreate()


def returnBadges():
    df = spark.read.csv(
        path='data/badges.csv',
        sep="\t",
        header=True,
        quote='"',
        schema="UserId INT, Name STRING, Date DATE, Class INT "
    )
    return df.rdd


def returnComments():
    df = spark.read.csv(
        path='data/comments.csv',
        sep="\t",
        header=True,
        quote='"',
        schema="PostId INT, Score INT, Text STRING, CreationDate DATE, UserId INT",
    )
    return df.rdd


def returnPosts():
    df = spark.read.csv(
        path='data/posts.csv',
        sep="\t",
        header=True,
        quote='"',
        inferSchema=True
        #schema="PostId INT, Score INT, Text STRING, CreationDate DATE, UserId INT",
    )
    return df.rdd


def returnUsers():
    df = spark.read.csv(
        path='data/users.csv',
        sep="\t",
        header=True,
        quote='"',
        inferSchema=True
        #schema="PostId INT, Score INT, Text STRING, CreationDate DATE, UserId INT",
    )
    return df.rdd


def main_method():
    returnUsers().collect()
    task1.main_task1()
    task2.main_task2()
    task3.main_task3()
    return


if __name__ == '__main__':
    main_method()
