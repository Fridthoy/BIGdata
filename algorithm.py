
import task1
from task1 import Rdd
from task2 import decodeString
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from graphframes import *
import re
from graphframes import *

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


# ----- see algorithm() for main function ------

def preProcessing(post):
    myposts = post.filter(lambda x: x[1] != ("-1" and "NULL"))
    post = myposts.map(decodeString)
    post = post.map(removeChar)
    return post


def removeChar(line: str):
    print("="*80)
    print("Unprocessed post")
    print("")
    print(line)
    print("="*80)
    line = line.lower()
    line = line.replace('<p>', ' ').replace(
        '</p>', '').replace('&#xa;', '').replace('\t', '')
    line = re.sub('[!?#$%&()=+<>;€Ÿ:/*@}{]', '', line)
    print("="*80)
    print("processed post")
    print("")
    print(line)
    print("="*80)
    return line


def tokenized(line: str):
    line = re.split('\s+', line)
    return line

# --- filtering out words with 3 or less letters ---


def sizeFilter(input_list):
    filteredList = []
    for x in input_list:
        if len(x) >= 3:
            x = re.sub('[.,]', ' ', x)  # removing DOT characters.
            x = re.sub(' ', '', x)
            filteredList.append(x)
    return filteredList

# --- removing words that are in the stopwords list --


def removeStopWords(input_list):
    updatedList = []
    for word in input_list:
        if word not in stopwords:
            updatedList.append(word)
    return updatedList

# --- removing word duplictates to make nodes ---


def createUniqueWordList(input_list):
    wordList = []
    for word in input_list:
        if word not in wordList:
            wordList.append(word)
    return wordList

# --- function for finding the edges ---


def findRelationships(tokens):
    windows = []
    for i in range(len(tokens)):
        if (i + 5 > len(tokens)):
            break
        window = []
        for j in range(5):
            window.append(tokens[i + j])
        windows.append(window)

    edges = createEdges(windows)
    print("="*80)
    print("Edges")
    print("")
    print(edges)
    print("="*80)
    return edges


def createEdges(windows):
    edges = []
    for window in windows:
        for element in window:
            for check in window:
                if element != check:
                    edge = (element, check)
                    if edge not in edges and (check, element) not in edges:
                        edges.append(edge)

    return edges


def algorithm(rdd, postID):
    # ----- initilizing the data -------
    postsRDD = rdd.getPosts()
    header = postsRDD.first()
    posts = postsRDD.filter(lambda x: x != header).map(lambda x: (x[0], x[5]))
    post = posts.filter(lambda x: x[0] == postID)
    post = post.map(lambda x: x[1])
    # ------- starting the preprocessing --------
    processedPost = preProcessing(post)
    tokenize = processedPost.map(tokenized)
    filteredTokens = tokenize.map(sizeFilter)
    processedTokens = filteredTokens.map(removeStopWords)

    print("="*80)
    print("Final sequence of tokens")
    print("")
    print(processedTokens.first())
    print("="*80)

    edges = processedTokens.map(findRelationships)
    uniqueList = processedTokens.map(createUniqueWordList)

    # ----- starting stage two -------
    edges = edges.first()
    vertices = uniqueList.first()
    print(vertices)
    # --- graphFrame ---
    #graphFrame(vertices, edges)

    return


def graphFrame(vertices, edges):
    spark, _ = rdd.init_spark()
    edgeDf = spark.createDataFrame(edges, ['src', 'dst'])
    verteciesDf = spark.createDataFrame(vertices, 'string').toDF('word')
    edgeDf.show()
    verteciesDf.show()
    g = GraphFrame(verteciesDf, edgeDf)
    g.degrees.show()
    pageRank = g.pageRank(resetProbability=0.15, tol=0.0001)
    pageRank.vertices.sort("page rank", desc=True).select(
        'word', "page rank").show(10)

    return


if __name__ == '__main__':
    rdd = Rdd()
    rdd.returnRddClass()
    algorithm(rdd, "14")

    '''
    list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    my = findRelationships(list)
    print(createEdges(my))
    print(my)
    '''
