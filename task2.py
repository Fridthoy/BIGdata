from task1 import Rdd

import base64
import datetime
import numpy as np


def convertStringToDate(line):

    line[2] = datetime.datetime.strptime(line[2], '%Y-%m-%d %H:%M:%S')
    return line


def findCommmentlength(commentrdd):

    header = commentrdd.first()

    lineLength = commentrdd.filter(lambda x: x != header).map(lambda x: x[2])

    lineLength = lineLength.map(lambda x: x.encode()).map(
        lambda x: base64.b64decode(x)).map(lambda x: x.decode())

    lineLength = lineLength.map(lambda x: len(x))

    print(lineLength.sum())


def questionsAndAnswers(postrdd):
    # return questions:
    questions = postrdd.filter(lambda x: x[1] == "1")
    answers = postrdd.filter(lambda x: x[1] == "2")
    print(answers.take(5))
    print(questions.take(5))


def task22(postrdd, userrdd):
    questions = postrdd.filter(lambda x: x[1] == "1")
    print(questions.take(3))
    dates = questions.map(convertStringToDate)
    print(dates.take(2))
    onlyDates = dates.map(lambda x: x[2])
    # dates when first and last questions where asked:
    maxDate = onlyDates.max()
    minDate = onlyDates.min()

    # users who posted these questions:

    usersIds = dates.filter(lambda x: x[2] == maxDate or x[2] == minDate).map(
        lambda x: x[6]).collect()

    namesOfUsers = userrdd.filter(
        lambda x: x[0] == usersIds[0] or x[0] == usersIds[1]).map(lambda x: x[3]).collect()

    print(namesOfUsers)


def task23(postrdd):
    header = postrdd.first()
    cleanId = postrdd.filter(lambda x: x != header).map(
        lambda x: x[6]).filter(lambda x: x != "-1")
    idCount = cleanId.countByValue()
    print(max(idCount, key=idCount.get))


def task24(badgerdd):
    header = badgerdd.first()
    fixedBadge = badgerdd.filter(lambda x: x != header).map(lambda x: x[0])
    idCount = fixedBadge.countByValue()
    lessThanTwo = dict(filter(lambda x: x[1] < 3, idCount.items()))

    print(len(lessThanTwo))

    return len(lessThanTwo)


def task25(userRdd):

    header = userRdd.first()
    fixedUser = userRdd.filter(lambda x: x != header)
    upVotes = fixedUser.map(lambda x: x[7]).map(lambda x: int(x))
    downVotes = fixedUser.map(lambda x: x[8]).map(lambda x: int(x))
    # first calculating the mean:
    meanUp = upVotes.mean()
    meanDown = downVotes.mean()

    downAndUp = fixedUser.map(lambda x: [int(x[i]) for i in [7, 8]])

    teller = downAndUp.map(lambda element: (
        element[0] - meanUp)*(element[1] - meanDown)).sum()
    sum1 = downAndUp.map(lambda element: (element[0] - meanUp)**2).sum()
    sum2 = downAndUp.map(lambda element: (element[1] - meanDown)**2).sum()
    r = teller/np.sqrt(sum1*sum2)

    print(r)


def task26(commentRdd):
    header = commentRdd.first()
    commentRdd = commentRdd.filter(lambda x: x != header)
    newCom = commentRdd.map(lambda x: x[4]).map(
        lambda x: [x, 1]).reduceByKey(lambda a, b: a+b)
    # calculating the entropy:
    records = commentRdd.count()  # total number of records
    prob = newCom.map(lambda x: x[1]/records)  # creating rdd with probobility
    print(prob.take(5))
    entropy = prob.map(lambda x: -x*np.log2(x)).sum()
    print(entropy)


def main_task2():
    rdd = Rdd()
    rdd.returnRddClass()

    # findCommmentlength(rdd.getComments())
    #task22(rdd.getPosts(), rdd.getusers())

    # task23(rdd.getPosts())
    # task24(rdd.getBadges())
    # task25(rdd.getusers())
    # task26(rdd.getComments())
    df = rdd.getusers().toDF()
    df.printSchema()
    df.show(truncate=False)

    return


if __name__ == '__main__':
    main_task2()
