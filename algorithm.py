
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

stopwords = ["a", "about", "above", "after", "again", "against", "ain", "all", "am",
             "an", "and", "any", "are", "aren", "aren't", "as", "at", "be", "because", "been", "before", "being",
             "below", "between", "both", "but", "by", "can", "couldn", "couldn't", "d", "did", "didn",
             "didn't", "do", "does", "doesn", "doesn't", "doing", "don", "don't", "down", "during",
             "each", "few", "for", "from", "further", "had", "hadn", "hadn't", "has", "hasn", "hasn't",
             "have", "haven", "haven't", "having", "he", "her", "here", "hers", "herself", "him",
             "himself", "his", "how", "i", "if", "in", "into", "is", "isn", "isn't", "it", "it's", "its",
             "itself", "just", "ll", "m", "ma", "me", "mightn", "mightn't", "more", "most", "mustn",
             "mustn't", "my", "myself", "needn", "needn't", "no", "nor", "not", "now", "o", "of", "off",
             "on", "once", "only", "or", "other", "our", "ours", "ourselves", "out", "over", "own", "re", "s", "same", "shan", "shan't", "she", "she's", "should", "should've", "shouldn",
             "shouldn't", "so", "some", "such", "t", "than", "that", "that'll", "the", "their",
             "theirs", "them", "themselves", "then", "there", "these", "they", "this", "those",
             "through", "to", "too", "under", "until", "up", "ve", "very", "was", "wasn", "wasn't", "we", "were", "weren", "weren't", "what", "when", "where", "which", "while", "who", "whom",
             "why", "will", "with", "won", "won't", "wouldn", "wouldn't", "y", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves", "could", "he'd", "he'll", "he's", "here's", "how's", "i'd", "i'll", "i'm", "i've", "let's", "ought", "she'd",
             "she'll", "that's", "there's", "they'd", "they'll", "they're", "they've", "we'd",
             "we'll", "we're", "we've", "what's", "when's", "where's", "who's", "why's", "would",
             "able", "abst", "accordance", "according", "accordingly", "across", "act", "actually",
             "added", "adj", "affected", "affecting", "affects", "afterwards", "ah", "almost",
             "alone", "along", "already", "also", "although", "always", "among", "amongst",
             "announce", "another", "anybody", "anyhow", "anymore", "anyone", "anything", "anyway",
             "anyways", "anywhere", "apparently", "approximately", "arent", "arise", "around",
             "aside", "ask", "asking", "auth", "available", "away", "awfully", "b", "back", "became",
             "become", "becomes", "becoming", "beforehand", "begin", "beginning", "beginnings",
             "begins", "behind", "believe", "beside", "besides", "beyond", "biol", "brief", "briefly", "c", "ca", "came", "cannot", "can't", "cause", "causes", "certain", "certainly", "co",
             "com", "come", "comes", "contain", "containing", "contains", "couldnt", "date",
             "different", "done", "downwards", "due", "e", "ed", "edu", "effect", "eg", "eight",
             "eighty", "either", "else", "elsewhere", "end", "ending", "enough", "especially", "et",
             "etc", "even", "ever", "every", "everybody", "everyone", "everything", "everywhere",
             "ex", "except", "f", "far", "ff", "fifth", "first", "five", "fix", "followed", "following", "follows", "former", "formerly", "forth", "found", "four", "furthermore", "g", "gave",
             "get", "gets", "getting", "give", "given", "gives", "giving", "go", "goes", "gone", "got",
             "gotten", "h", "happens", "hardly", "hed", "hence", "hereafter", "hereby", "herein",
             "heres", "hereupon", "hes", "hi", "hid", "hither", "home", "howbeit", "however",
             "hundred", "id", "ie", "im", "immediate", "immediately", "importance", "important",
             "inc", "indeed", "index", "information", "instead", "invention", "inward", "itd",
             "it'll", "j", "k", "keep", "keeps", "kept", "kg", "km", "know", "known", "knows", "l",
             "largely", "last", "lately", "later", "latter", "latterly", "least", "less", "lest",
             "let", "lets", "like", "liked", "likely", "line", "little", "'ll", "look", "looking",
             "looks", "ltd", "made", "mainly", "make", "makes", "many", "may", "maybe", "mean", "means", "meantime", "meanwhile", "merely", "mg", "might", "million", "miss", "ml", "moreover",
             "mostly", "mr", "mrs", "much", "mug", "must", "n", "na", "name", "namely", "nay", "nd",
             "near", "nearly", "necessarily", "necessary", "need", "needs", "neither", "never",
             "nevertheless", "new", "next", "nine", "ninety", "nobody", "non", "none", "nonetheless",
             "noone", "normally", "nos", "noted", "nothing", "nowhere", "obtain", "obtained",
             "obviously", "often", "oh", "ok", "okay", "old", "omitted", "one", "ones", "onto", "ord",
             "others", "otherwise", "outside", "overall", "owing", "p", "page", "pages", "part",
             "particular", "particularly", "past", "per", "perhaps", "placed", "please", "plus",
             "poorly", "possible", "possibly", "potentially", "pp", "predominantly", "present",
             "previously", "primarily", "probably", "promptly", "proud", "provides", "put", "q",
             "que", "quickly", "quite", "qv", "r", "ran", "rather", "rd", "readily", "really", "recent", "recently", "ref", "refs", "regarding", "regardless", "regards", "related",
             "relatively", "research", "respectively", "resulted", "resulting", "results", "right",
             "run", "said", "saw", "say", "saying", "says", "sec", "section", "see", "seeing", "seem",
             "seemed", "seeming", "seems", "seen", "self", "selves", "sent", "seven", "several",
             "shall", "shed", "shes", "show", "showed", "shown", "showns", "shows", "significant",
             "significantly", "similar", "similarly", "since", "six", "slightly", "somebody",
             "somehow", "someone", "somethan", "something", "sometime", "sometimes", "somewhat",
             "somewhere", "soon", "sorry", "specifically", "specified", "specify", "specifying",
             "still", "stop", "strongly", "sub", "substantially", "successfully", "sufficiently",
             "suggest", "sup", "sure", "take", "taken", "taking", "tell", "tends", "th", "thank",
             "thanks", "thanx", "thats", "that've", "thence", "thereafter", "thereby", "thered",
             "therefore", "therein", "there'll", "thereof", "therere", "theres", "thereto",
             "thereupon", "there've", "theyd", "theyre", "think", "thou", "though", "thoughh",
             "thousand", "throug", "throughout", "thru", "thus", "til", "tip", "together", "took",
             "toward", "towards", "tried", "tries", "truly", "try", "trying", "ts", "twice", "two", "u", "un", "unfortunately", "unless", "unlike", "unlikely", "unto", "upon", "ups", "us",
             "use", "used", "useful", "usefully", "usefulness", "uses", "using", "usually", "v",
             "value", "various", "'ve", "via", "viz", "vol", "vols", "vs", "w", "want", "wants", "wasnt", "way", "wed", "welcome", "went", "werent", "whatever", "what'll", "whats", "whence",
             "whenever", "whereafter", "whereas", "whereby", "wherein", "wheres", "whereupon",
             "wherever", "whether", "whim", "whither", "whod", "whoever", "whole", "who'll",
             "whomever", "whos", "whose", "widely", "willing", "wish", "within", "without", "wont",
             "words", "world", "wouldnt", "www", "x", "yes", "yet", "youd", "youre", "z", "zero", "a's",
             "ain't", "allow", "allows", "apart", "appear", "appreciate", "appropriate",
             "associated", "best", "better", "c'mon", "c's", "cant", "changes", "clearly",
             "concerning", "consequently", "consider", "considering", "corresponding", "course",
             "currently", "definitely", "described", "despite", "entirely", "exactly", "example",
             "going", "greetings", "hello", "help", "hopefully", "ignored", "inasmuch", "indicate",
             "indicated", "indicates", "inner", "insofar", "it'd", "keep", "keeps", "novel",
             "presumably", "reasonably", "second", "secondly", "sensible", "serious", "seriously",
             "sure", "t's", "third", "thorough", "thoroughly", "three", "well", "wonder"]

# We need to filter on postIDs
# problem, cant use the standard decoder made in task2, need to take in an RDD


def preProcessing(post):
    myposts = post.filter(lambda x: x[1] != ("-1" and "NULL"))
    post = myposts.map(decodeString)
    post = post.map(removeChar)
    return post


def removeChar(line: str):
    print(line)
    line = line.lower()
    line = line.replace('<p>', ' ').replace(
        '</p>', '').replace('&#xa;', '').replace('\t', '')
    line = re.sub('[!?#$%&()=+<>;€Ÿ:/*@}{]', '', line)
    print("="*80)
    print(line)

    return line


def tokenized(line: str):
    line = re.split("\s+", line)
    return line


def biggerThan3(list):
    mylist = []
    for x in list:
        if len(x) >= 3:
            x = re.sub('[.,]', ' ', x)
            x = re.sub(' ', '', x)
            mylist.append(x)
    print(mylist)
    return mylist


def removeStopWords(list):
    myList = []
    for word in list:
        if word not in stopwords:
            myList.append(word)
    return myList


def createUniqueWordList(list):
    myList = []
    for word in list:
        if word not in myList:
            myList.append(word)
    return myList


def findRelationships(tokens):
    myWindows = []
    for i in range(len(tokens)):
        if (i + 5 > len(tokens)):
            break
        window = []
        for j in range(5):
            window.append(tokens[i + j])
        myWindows.append(window)

    myEdges = createEdges(myWindows)
    return myEdges


def createEdges(windows):
    edges = []
    for winds in windows:
        for win in winds:
            for check in winds:
                if win != check:
                    edge = (win, check)
                    if edge not in edges and (check, win) not in edges:
                        edges.append(edge)

    return edges


def algorithm(postsRDD, postID):
    header = postsRDD.first()
    posts = postsRDD.filter(lambda x: x != header).map(lambda x: (x[0], x[5]))
    post = posts.filter(lambda x: x[0] == postID)
    post = post.map(lambda x: x[1])
    processedPost = preProcessing(post)
    myTok = processedPost.map(tokenized)
    myTok = myTok.map(biggerThan3)
    myTok = myTok.map(removeStopWords)

    myWindows = myTok.map(findRelationships)
    print(myWindows.take(3))
    uniqueList = myTok.map(createUniqueWordList)

    # biggerThan3 = myTok.filter(lambda x: len(x)>=3)

    return


if __name__ == '__main__':
    rdd = Rdd()
    rdd.returnRddClass()
    algorithm(rdd.getPosts(), "14")

    '''
    list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    my = findRelationships(list)
    print(createEdges(my))
    print(my)

    '''
