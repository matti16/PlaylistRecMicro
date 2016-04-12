import string


def tokenize_song(x):
    out = ''.join(ch for ch in x if ch not in string.punctuation)
    tokens = [i for i in out.split(" ") if len(i) and not i in string.punctuation]
    return [t.lower() for t in tokens]
    

def jaccard_similarity(x, y):
    num = len( set(x).intersection(set(y)) )
    den = len( set(x).union(set(y)) )
    return num / den
    

def classify(x, y, treshold = 0.5):
    x = tokenize_song(x)
    y = tokenize_song(y)
    return 1 if jaccard_similarity(x, y) >= treshold else 0