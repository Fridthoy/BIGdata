from task1 import Rdd


import base64
import datetime


def convertStringToDate(line: str):

    line[2] = datetime.datetime.strptime(line[2], '%Y-%m-%d %H:%M:%S')
    return line

def findCommmentlength(commentrdd):

    header = commentrdd.first()

    lineLength = commentrdd.filter(lambda x: x != header).map(lambda x: x[2])

    lineLength = lineLength.map(lambda x: x.encode()).map(lambda x: base64.b64decode(x)).map(lambda x: x.decode())

    lineLength = lineLength.map(lambda x: len(x))

    print(lineLength.sum())


def questionsAndAnswers(postrdd):
    #return questions:
    questions = postrdd.filter(lambda x: x[1] == "1")
    answers = postrdd.filter(lambda x: x[1]== "2")
    print(answers.take(5))
    print(questions.take(5))

def task22(postrdd, userrdd):
    questions = postrdd.filter(lambda x: x[1] == "1")
    dates = questions.map(convertStringToDate)
    onlyDates = dates.map(lambda x: x[2])
    #dates when first and last questions where asked:
    maxDate = onlyDates.max()
    minDate = onlyDates.min()

    #users who posted these questions:

    usersIds = dates.filter(lambda x: x[2] == maxDate or x[2] == minDate).map(lambda x: x[6]).collect()

    namesOfUsers = userrdd.filter(lambda x: x[0] == usersIds[0] or x[0] == usersIds[1]).map(lambda x: x[3]).collect()

    print(namesOfUsers)


if __name__ == '__main__':
    rdd = Rdd()
    rdd.returnRddClass()

    #findCommmentlength(rdd.getComments())
    task22(rdd.getPosts(), rdd.getusers())
