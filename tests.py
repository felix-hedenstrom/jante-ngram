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

    ngm3.insert("this is a test. tests are cool.")

    ngm3.insert("this kinda cool. this will be good if it works.")
    ngm3.insert("this is the first time I try this I promise")

    #with open("43-0.txt") as f:
    import urllib.request
    with urllib.request.urlopen("https://www.gutenberg.org/files/43/43-0.txt") as f:

        text = f.read().decode('utf-8')


        ngm3.insert(text)
        ngm1.insert(text)

    import random
    import time

    start = time.time()
    print(ngm3.generate("this", "you", min_length=10, limit=20, rand=True))
    print(ngm3.generate("this", min_length=5, limit=10))
    print(ngm3.generate("this is", min_length=7, max_length=7))
    elapsed = time.time() - start

    print(elapsed)

    all_3grams = ngm3.get_all() 
    all_1grams = ngm1.get_all()

    assert len(all_1grams) > 1
    assert len(all_3grams) > 1

    assert ('this', 'is', 'a') in all_3grams 
    assert ('Gutenberg',) in all_1grams 
