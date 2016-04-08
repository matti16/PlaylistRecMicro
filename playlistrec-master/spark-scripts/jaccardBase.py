import nltk
import string

def tokenize_song(x):
    out = x.translate(x.maketrans("","", string.punctuation))
    tokens = nltk.word_tokenize(out)
    return [t.lower() for t in tokens]
    

def jaccard_similarity(x, y):
    num = len( set(x).intersection(set(y)) )
    den = len( set(x).union(set(y)) )
    return num / den
    

def classify(x, y, treshold = 0.5):
    x = tokenize_song(x)
    y = tokenize_song(y)
    return 1 if jaccard_similarity(x, y) >= treshold else 0