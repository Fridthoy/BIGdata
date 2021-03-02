from pyspark.sql import SparkSession
import findspark
findspark.init()


class Rdd:

    def init_spark(self, app_name="FirstApp", execution_mode="local[*]"):
        spark = SparkSession.builder.master(
            execution_mode).appName(app_name).getOrCreate()
        sc = spark.sparkContext
        sc.setLogLevel("ERROR")
        return spark, sc

    def returnRddClass(self):
        _, sc = self.init_spark()
        self.badges = sc.textFile('data/badges.csv').map(
            lambda element: element.split('\t')).cache()
        self.comments = sc.textFile('data/comments.csv').map(
            lambda element: element.split('\t')).cache()
        self.posts = sc.textFile('data/posts.csv').map(
            lambda element: element.split('\t')).cache()
        self.users = sc.textFile('data/users.csv').map(
            lambda element: element.split('\t')).cache()

    def getBadges(self):
        return self.badges

    def getComments(self):
        return self.comments

    def getPosts(self):
        return self.posts

    def getusers(self):
        return self.users


def findNumberOfRows(rdd):

    print("badges has ", rdd.getBadges().count(), " rows")
    print("Comments has ", rdd.getComments().count(), " rows")
    print("Posts has ", rdd.getPosts().count(), " rows")
    print("Users has ", rdd.getusers().count(), " rows")


def main_task1():
    rdd = Rdd()
    rdd.returnRddClass()

    findNumberOfRows(rdd)
    return


if __name__ == '__main__':
    main_task1()
