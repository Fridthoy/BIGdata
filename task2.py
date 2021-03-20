from task1 import Rdd

import base64
import datetime
import numpy as np


def convertStringToDate(line: str):
    line[2] = datetime.datetime.strptime(line[2], '%Y-%m-%d %H:%M:%S')
    return line


def decodeString(line: str):
    line = line.encode()
    line = base64.b64decode(line)
    line = line.decode()
    return line


def task21(commentrdd, postrdd):

    header = commentrdd.first()
    comments = commentrdd.filter(lambda x: x != header).map(lambda x: x[2])
    questions = postrdd.filter(lambda x: x[1] == "1").map(lambda x: x[5])
    answers = postrdd.filter(lambda x: x[1] == "2").map(lambda x: x[5])
    commentRecords = comments.count()
    questionRecords = questions.count()
    answersRecords = answers.count()
    commentDecode = comments.map(decodeString)
    questionDecode = questions.map(decodeString)
    answerDecode = answers.map(decodeString)
    commentsLength = commentDecode.map(lambda x: len(x))
    questionLength = questionDecode.map(lambda x: len(x))
    answerLength = answerDecode.map(lambda x: len(x))
    avgComment = commentsLength.sum()/commentRecords
    avgQuestion = questionLength.sum()/questionRecords
    avgAnswer = answerLength.sum()/answersRecords
    print("---------------- task 2.1 -----------------------")
    print(" ")
    print("Average length of comments: ", avgComment)
    print("Average length of questions: ", avgQuestion)
    print("Average length of answers: ", avgAnswer)
    print("")
    print("-------------------------------------------------")
    return


def task22(postrdd, userrdd):
    questions = postrdd.filter(lambda x: x[1] == "1")
    dates = questions.map(convertStringToDate)
    onlyDates = dates.map(lambda x: x[2])
    # dates when first and last questions where asked:
    maxDate = onlyDates.max()
    minDate = onlyDates.min()
    # users who posted these questions:
    usersIds = dates.filter(lambda x: x[2] == maxDate or x[2] == minDate).map(
        lambda x: x[6]).collect()
    namesOfUsers = userrdd.filter(
        lambda x: x[0] == usersIds[0] or x[0] == usersIds[1]).map(lambda x: x[3]).collect()
    print("---------------- task 2.2 -----------------------")
    print("")
    print("The last question: ", maxDate)
    print("The first question: ", minDate)
    print("The users who posted these questions: ", namesOfUsers)
    print("")
    print("-------------------------------------------------")
    return


def task23(postrdd):
    header = postrdd.first()
    cleanPost = postrdd.filter(lambda x: x != header)

    question = cleanPost.filter(lambda x: x[1] == "1").map(
        lambda x: x[6]).filter(lambda x: x != "NULL")
    answer = cleanPost.filter(lambda x: x[1] == "2").map(
        lambda x: x[6]).filter(lambda x: x != "NULL")

    question = question.map(lambda x: (x, 1)).reduceByKey(
        lambda x, y: x+y).sortBy(lambda x: -x[1])
    answer = answer.map(lambda x: (x, 1)).reduceByKey(
        lambda x, y: x+y).sortBy(lambda x: -x[1])

    print("---------------- task 2.3 -----------------------")
    print(" ")
    print("The id's of the users who wrote the greatest number of questions and answers:")
    print("Answers: ", answer.first()[1], " --- ID: ", answer.first()[0])
    print("Questions: ", question.first()[1], " --- ID: ", question.first()[0])
    print(" ")
    print("-------------------------------------------------")
    return


def task24(badgerdd, userrdd):
    print("---------------- task 2.4 -----------------------")
    print(" ")
    header = badgerdd.first()
    fixedBadge = badgerdd.filter(lambda x: x != header).map(lambda x: x[0])
    idCount = fixedBadge.countByValue()
    lessThanTwo = dict(filter(lambda x: x[1] < 3, idCount.items()))
    badgeId = fixedBadge.distinct().count()
    total = (userrdd.count()-1 - badgeId) + len(lessThanTwo)

    print(total)
    print("counting complete")
    print(" ")
    print("Number of users who hve received less than three badges: ",
          total)
    print(" ")
    print("-------------------------------------------------")

    print(total)

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
    print("---------------- task 2.5 -----------------------")
    print(" ")
    print("The Pearson correlation coefficent: ", r)
    print(" ")
    print("-------------------------------------------------")
    return


def task26(commentRdd):
    header = commentRdd.first()
    commentRdd = commentRdd.filter(lambda x: x != header)
    newCom = commentRdd.map(lambda x: x[4]).map(
        lambda x: [x, 1]).reduceByKey(lambda a, b: a+b)
    # calculating the entropy:
    records = commentRdd.count()  # total number of records
    prob = newCom.map(lambda x: x[1]/records)  # creating rdd with probobility
    # print(prob.take(5))
    entropy = prob.map(lambda x: -x*np.log2(x)).sum()
    print("---------------- task 2.6 -----------------------")
    print(" ")
    print("The Entropy of id of users: ", entropy)
    print(" ")
    print("-------------------------------------------------")
    return


def main_task2():
    rdd = Rdd()
    rdd.returnRddClass()

    task21(rdd.getComments(), rdd.getPosts())
    task22(rdd.getPosts(), rdd.getusers())

    task23(rdd.getPosts())
    task24(rdd.getBadges(), rdd.getusers())
    task25(rdd.getusers())
    task26(rdd.getComments())
    # df = rdd.getusers().toDF()
    # df.printSchema()
    # df.show(truncate=False)
    print(" ")
    print("------------- TASK 2 COMPLETED -------------")
    return


if __name__ == '__main__':
    main_task2()
