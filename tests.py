#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import ngram
import sqlite3
import os
import unittest

max_test_n = 5

class TestNGram(unittest.TestCase):

    def test_basic(self):
        for i in range(2, max_test_n):
            ngm = ngram.NGramManager(":memory:", i)

            ngm.insert("this is a test. tests are cool.")

            self.assertEqual(1, len(ngm.generate("this", "cool.")))  

            ngm.insert("this is also very cool.")
            self.assertEqual(2, len(ngm.generate("this", "cool.")))

    def test_threading(self):

        import threading

        for i in range(2, max_test_n):
            ngm = ngram.NGramManager(":memory:", i)

            # Using sqlite3 connections without check_same_thread results in an error when trying to access it from a thread other than the one that created it
            # Since check_same_thread = True we should get no such error and everything should work as we expect

            thread1 = threading.Thread(target=lambda: ngm.insert("Hello thread1 here!"))
            thread2 = threading.Thread(target=lambda: ngm.insert("Hello thread2 here!"))

            thread1.start()
            thread2.start()

            thread1.join()
            thread2.join()

            self.assertEqual(set(["Hello thread1 here!", "Hello thread2 here!"]), set(ngm.generate("Hello", "here!")))

    def test_general_ending(self):
        for i in range(2, max_test_n):
            ngm = ngram.NGramManager(":memory:", i)

            ngm.insert("This sentence ends with a 1")
            ngm.insert("This sentence ends with a 2")
            ngm.insert("This sentence ends with a 3")

            self.assertEqual(3, len(ngm.generate("This")))


    def test_general_beginning(self):
        for i in range(2, max_test_n):
            ngm = ngram.NGramManager(":memory:", i)

            ngm.insert("This sentence ends with a 1")
            ngm.insert("This sentence ends with a 2")
            ngm.insert("This sentence ends with a 3")

            self.assertEqual(3, len(ngm.generate()))

    def test_limit(self):
        ngm = ngram.NGramManager(":memory:", 3)

        ngm.insert("This sentence ends with a 1")
        self.assertEqual(1, len(ngm.generate("This", limit=2)))

        ngm.insert("This sentence ends with a 2")
        self.assertEqual(2, len(ngm.generate("This", limit=2)))

        ngm.insert("This sentence ends with a 3")
        # There are three ways to finish but since we limit it to 2 we should always get 2
        self.assertEqual(2, len(ngm.generate("This", limit=2)))
        self.assertEqual(1, len(ngm.generate("This", limit=1)))

    def test_partial_sentence(self):
        # Not applicable to 2Gram since they start with a seed of length 1
        for i in range(3, max_test_n):
            ngm = ngram.NGramManager(":memory:", i)

            ngm.insert("Hello I love writing words. I enjoy it because it makes me feel happy!") 

            # When looking for full sentences, we should not find anything, since the only start token is "Hello" and the only end token is "happy!" 
            self.assertEqual(0, len(ngm.generate("I love", "it")))

            self.assertEqual(set(['I love writing words. I enjoy it', 'I love writing words. I enjoy it because it']), set(ngm.generate("I love", "it", partial=True)))
            self.assertEqual(set(['I enjoy it because it', 'I love writing words. I enjoy it', 'I love writing words. I enjoy it because it']), set(ngm.generate("I", "it", partial=True)))
            self.assertTrue(len(ngm.generate(partial=True)) > 1)

if __name__ == "__main__":
    unittest.main()

if __name__ == "__main__" and False:
    

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

