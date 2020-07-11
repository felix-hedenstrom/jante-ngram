import ngram
import sqlite3

if __name__ == "__main__":

    path = ":memory:"

    ngm = ngram.NGramManager(path)

    ngm.insert("this is a test. tests are cool.")

    ngm.insert("this kinda cool. this will be good if it works.")
    ngm.insert("this is the first time I try this I promise")
    print(ngm.generate("this"))
    print(ngm.generate())
    print(ngm.generate("this is"))
