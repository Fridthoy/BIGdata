import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark import SparkConf



spark = SparkSession.builder.master("local").appName("badgesLoad").getOrCreate()


def returnBadges():
    df = spark.read.csv(
        path = 'badges.csv',
        sep= "\t",
        header= True,
        quote='"',
        schema="UserId INT, Name STRING, Date DATE, Class INT "
    )
    return df.rdd


def returnComments():
    df = spark.read.csv(
        path = 'comments.csv',
        sep= "\t",
        header= True,
        quote='"',
        schema="PostId INT, Score INT, Text STRING, CreationDate DATE, UserId INT",
    )
    return df.rdd

def returnPosts():
    df = spark.read.csv(
        path = 'posts.csv',
        sep= "\t",
        header= True,
        quote='"',
        inferSchema= True
        #schema="PostId INT, Score INT, Text STRING, CreationDate DATE, UserId INT",
    )
    return df.rdd

def returnUsers():
    df = spark.read.csv(
        path = 'users.csv',
        sep= "\t",
        header= True,
        quote='"',
        inferSchema= True
        #schema="PostId INT, Score INT, Text STRING, CreationDate DATE, UserId INT",
    )
    return df.rdd

if __name__ == '__main__':
    returnUsers().collect()