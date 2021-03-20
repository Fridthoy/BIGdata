'''
The algorithm consists of two major stages.
Given a piece of textual information like a web-post, the algorithm constructs a graph of the terms in the document
first. In this graph, terms in the input document are nodes, and the edges show the relationship between the terms.
Then, it will rank nodes (or terms) based on their PageRank scores. Finally, it will return top-k terms with the most
PageRank score as the input document's representative terms.

'''

import task1
from task1 import Rdd
from task2 import decodeString
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
import base64


# We need to filter on postIDs
# problem, cant use the standard decoder made in task2, need to take in an RDD

def preProcessing(post):
    myposts = post.filter(lambda x: x[1] != ("-1" and "NULL"))
    post = myposts.map(decodeString)
    print(myposts.take(1))
    # postss = myposts.map(lambda x: x[1])
    # post = postss.map(decodeString)
    post = post.map(removeChar)
    print(post.take(1))
    # post = post.lower()
    # print(post)
    # post.sub(['!?#$%&()=+'])

    return post


def removeChar(line: str):
    line = line.lower()
    line = re.sub('[!?#$%&()=+<>;:/*@]', '', line)
    return line


def tokenize():

    return


def algorithm(postsRDD, postID):
    header = postsRDD.first()
    posts = postsRDD.filter(lambda x: x != header).map(lambda x: (x[0], x[5]))
    post = posts.filter(lambda x: x[0] == postID)
    post = post.map(lambda x: x[1])
    print(post.take(1))
    # posts = posts.toDF()
    # posts.show()
    # df.select(df['_2']).printSchema()
    # df.select(lower(col(df['_2']))).show()
    processedPost = preProcessing(post)

    return


if __name__ == '__main__':
    rdd = Rdd()
    rdd.returnRddClass()
    algorithm(rdd.getPosts(), "14")
