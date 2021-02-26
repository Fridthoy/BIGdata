from task1 import Rdd
import base64


def decode(rdd):
    decoded = rdd.map(lambda x: x.encode('ascii')).map(
        lambda x: base64.b64decode(x)).map(lambda x: x.decode('ascii'))

    return decoded


def findCommmentlength(commentrdd):

    header = commentrdd.first()

    lineLength = commentrdd.filter(lambda x: x != header).map(lambda x: x[2])
    lineLength = decode(lineLength)
    lineLength = lineLength.map(lambda x: len(x)).sum()

    return lineLength/commentrdd.count()


if __name__ == '__main__':
    rdd = Rdd()
    rdd.returnRddClass()

    print(findCommmentlength(rdd.getComments()))

    # print(rdd.getComments().take(2))
