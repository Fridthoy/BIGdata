from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("badgesLoad").getOrCreate()


def returnBadges():
    df = spark.read.csv(
        path = 'badges.csv',
        sep= "\t",
        header= True,
        quote='"',
        schema="UserId INT, Name STRING, Date DATE, Class INT "
    )
    print("yolo")
    return df


def returnComments():
    df = spark.read.csv(
        path = 'comments.csv',
        sep= "\t",
        header= True,
        quote='"',
        schema="PostId INT, Score INT, Text STRING, CreationDate DATE, UserId INT",
    )
    return df

def returnPosts():
    df = spark.read.csv(
        path = 'posts.csv',
        sep= "\t",
        header= True,
        quote='"',
        inferSchema= True
        #schema="PostId INT, Score INT, Text STRING, CreationDate DATE, UserId INT",
    )
    return df

def returnUsers():
    df = spark.read.csv(
        path = 'users.csv',
        sep= "\t",
        header= True,
        quote='"',
        inferSchema= True
        #schema="PostId INT, Score INT, Text STRING, CreationDate DATE, UserId INT",
    )
    return df

if __name__ == '__main__':
    returnUsers().show(20)