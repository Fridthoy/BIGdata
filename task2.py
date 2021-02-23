from task1 import Rdd

def findCommmentlength(commentrdd):

    print(commentrdd.take(5))

    lineLength = commentrdd.map(lambda s: len(s))


    print(lineLength.take(5))





if __name__ == '__main__':
    rdd = Rdd()
    rdd.returnRddClass()

    findCommmentlength(rdd.getComments())

    #print(rdd.getComments().take(2))