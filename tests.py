#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import ngram
import sqlite3
import os

if __name__ == "__main__":
    
    path = "/tmp/test.db"

    try:
        os.remove(path)
    except FileNotFoundError:
        pass

    ngm1 = ngram.NGramManager(path, 1)
    ngm3 = ngram.NGramManager(path, 3)
    ngm4 = ngram.NGramManager(path, 4)

    ngm3.insert("this is a test. tests are cool.")

    ngm3.insert("this kinda cool. this will be good if it works.")
    ngm3.insert("this is the first time I try this I promise")

    import urllib.request
    with open("43-0.txt") as f:
    #with urllib.request.urlopen("https://www.gutenberg.org/files/43/43-0.txt") as f:

        #text = f.read().decode('utf-8')
        text = f.read()


        ngm3.insert(text)
        ngm4.insert(text)
        ngm1.insert(text)

    import random
    import time

    start = time.time()
    print(ngm3.generate("this", "you", min_length=10, limit=20, rand=True))
    print(ngm3.generate("this", min_length=5, limit=10))
    print(ngm3.generate("this is", min_length=7, max_length=7,))
    assert len(ngm3.generate("this is", min_length=7, max_length=7, limit=20)) == 20
    elapsed = time.time() - start

    print(elapsed)

    all_1grams = ngm1.get_all()
    all_3grams = ngm3.get_all() 
    all_4grams = ngm4.get_all()

    assert len(all_1grams) > 1
    assert len(all_3grams) > 1
    assert len(all_4grams) > 1

    print(list(filter(lambda x: x[0] == "this" and x[1] == "is", all_4grams)))
    print(ngm4.generate("", min_length=1, max_length=10,limit=4))
    print(ngm4.generate("this agreement", min_length=1, max_length=10,limit=10))
    print(ngm4.generate("Mr. Utterson", min_length=1, max_length=10,limit=10))
    print(ngm4.generate("this is", min_length=1, max_length=10,limit=4))

    assert ('this', 'is', 'a') in all_3grams 
    assert ('Gutenberg',) in all_1grams 

    connection = sqlite3.connect(path)
    
    cursor = connection.cursor()

    print(sorted(filter(lambda x: x[0] == 'this' and x[1] == 'is' and x[2] == 'a', all_4grams)))
    print(ngm4.generate("i think this is a", ending="me any", min_length=1, max_length=20,limit=4))
    print(ngm4.generate("i think this is a", ending="spare me any ill will", min_length=1, max_length=20,limit=4))
    print(ngm3.generate("i think this is a", ending="me any friends that i know", min_length=1, max_length=20,limit=1))

    print(cursor.fetchall())

    connection.close()

