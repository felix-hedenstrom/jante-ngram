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
            # Since check_same_thread = False we should get no such error and everything should work as we expect

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

    def test_long_seed(self):
        ngm = ngram.NGramManager(":memory:", 3)
        ngm.insert("This is a longer sentence that has an ending that is interesting")

        self.assertEqual("Here is a longer sentence that has an ending that is interesting", ngm.generate("Here is a longer")[0])
        self.assertEqual("Here is a longer sentence that has an ending that is interesting", ngm.generate("Here is a longer", ending="interesting")[0])

        self.assertEqual("Here is a longer sentence that has an ending that is interesting", ngm.generate("Here is a longer", ending="interesting", partial=True)[0])
        self.assertEqual("Here is a longer sentence that has", ngm.generate("Here is a longer", ending="has", partial=True)[0])

    def test_long_ending(self):
        ngm = ngram.NGramManager(":memory:", 3)
        ngm.insert("This is a longer sentence that has an ending that is interesting")

        self.assertEqual("This is a longer sentence that has an ending that is interesting because it seems fun", ngm.generate(ending="is interesting because it seems fun")[0])
        self.assertEqual("This is a longer sentence that has an ending that is interesting because it seems fun", ngm.generate("This", "is interesting because it seems fun")[0])

        # Last word should be the word "be"
        self.assertEqual("be", ngm.generate(ending="is interesting because it needs to be", partial=True)[0].split()[-1])

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
