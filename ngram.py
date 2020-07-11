import sqlite3
import nltk

LEFT_PAD_SYMBOL = "<s>"
RIGHT_PAD_SYMBOL = "</s>"

class NGramManager:
    def __init__(self, path):

        self._db_path = path

        self._connection = sqlite3.connect(self._db_path)

        cursor = self._connection.cursor() 

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS NGram 
        (
            TokenText1 TEXT NOT NULL,
            TokenText2 TEXT NOT NULL,
            TokenText3 TEXT NOT NULL,
            PRIMARY KEY  (TokenText1, TokenText2, TokenText3)
        )
        """)
        
        self._connection.commit()

    def generate_ngrams(text, n, pad_right=True, pad_left=True):
        return list(nltk.ngrams(
                nltk.pad_sequence(
                    text.split(),
                    pad_left=pad_left, left_pad_symbol=LEFT_PAD_SYMBOL,
                    pad_right=pad_right, right_pad_symbol=RIGHT_PAD_SYMBOL,
                    n=n),
                 n=n))

    def generate(self, seed=""):

        cursor = self._connection.cursor() 

        ngrams = NGramManager.generate_ngrams(seed, n=3, pad_right=False)

        if len(ngrams) == 0:
            last_ngram = ["<s>", "<s>", "<s>"]
        else:
            last_ngram = ngrams[-1]

        cursor.execute("""
        SELECT
            TokenText1,
            TokenText2,
            TokenText3
        FROM
            NGram
        WHERE
            (
                TokenText1 = :word2 
                OR 
                :word1 IS NULL
            )
            AND
            (
                TokenText2 = :word3
                OR
                :word2 IS NULL
            )

        """, {"word1": last_ngram[0], "word2": last_ngram[1], "word3": last_ngram[2]})

        return cursor.fetchall()


    def insert(self, text):

        cursor = self._connection.cursor() 
        
            
        ngrams = NGramManager.generate_ngrams(text, 3)

        cursor.executemany("""
        INSERT OR IGNORE INTO NGram
        (
            TokenText1,
            TokenText2,
            TokenText3
        )
        VALUES
        (
            ?,
            ?,
            ?
        )""", ngrams)

        self._connection.commit()
        
        

