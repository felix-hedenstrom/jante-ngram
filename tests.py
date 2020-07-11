import ngram
import sqlite3

if __name__ == "__main__":

    #path = "/tmp/test.db"
    path = ":memory:"

    ngm = ngram.NGramManager(path)

    ngm.insert("this is a test. tests are cool.")

    ngm.insert("this kinda cool. this will be good if it works.")
    ngm.insert("this is the first time I try this I promise")

    #with open("43-0.txt") as f:
    import urllib.request
    with urllib.request.urlopen("https://www.gutenberg.org/files/43/43-0.txt") as f:

        text = f.read().decode('utf-8')


        ngm.insert(text)

    import random
    import time

    start = time.time()
    print(ngm.generate("this", "you", min_length=5, limit=20))
    print(ngm.generate("this is", min_length=7, max_length=7))
    elapsed = time.time() - start

    print(elapsed)
