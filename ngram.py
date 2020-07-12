#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sqlite3
import nltk
import os

LEFT_PAD_SYMBOL = "<s>"
RIGHT_PAD_SYMBOL = "</s>"
NEWLINE = os.linesep 

class NGramManager:
    def __init__(self, path, n=3):

        if not type(n) == int:
            return ValueError("n must be an integer")

        self._db_path = path

        connection = sqlite3.connect(self._db_path)

        cursor = connection.cursor() 

        self._n = n

        sql = f"""
        CREATE TABLE IF NOT EXISTS NGram{n} 
        (

            {("," + NEWLINE).join(
                map(
                    lambda i: 
                        "TokenText" + str(i) + " TEXT NOT NULL", 
                    range(1, self._n + 1)))}
            ,PRIMARY KEY ({",".join(
                                map(
                                    lambda i: f"TokenText{i}", 
                                    range(1, n + 1)))})
        )"""

        cursor.execute(sql)
        
        connection.commit()
        connection.close()

    def generate_ngrams(text, n, pad_right=True, pad_left=True, right_pad_symbol=RIGHT_PAD_SYMBOL, left_pad_symbol=LEFT_PAD_SYMBOL):
        return list(nltk.ngrams(
                nltk.pad_sequence(
                    text.split(),
                    pad_left=pad_left, left_pad_symbol=left_pad_symbol,
                    pad_right=pad_right, right_pad_symbol=right_pad_symbol,
                    n=n),
                 n=n))
    
    def get_all(self):

        connection = sqlite3.connect(self._db_path)

        cursor = connection.cursor() 

        sql = f"""
        SELECT
            {",".join(map(lambda i: "TokenText" + str(i), range(1, self._n + 1)))} 
        FROM
            NGram{self._n}
        """

        cursor.execute(sql)

        try:
            return cursor.fetchall()
        finally: 
            connection.close()

    def generate(self, seed="", ending="", max_length=None, min_length=4, limit=1, row_limit=10000000, rand=False):
        """Generate sentances from the ngrams

        Generate a text based on chained ngrams

        Parameters

        seed : str
            Word(s) that will start the chain
        ending : str
            Word(s) that will end the chain
        max_length : int
        min_length : int
            should be smaller or equal to max_length
        limit : int
            max number of returned ngrams
        row_limit : int
            max number of ngrams that will be kept in memory.
        """

        if max_length is None:
            max_length = min_length + 10

        if max_length < min_length:
            raise ValueError("max_length cannot be lower than min_length") 

        connection = sqlite3.connect(self._db_path)

        cursor = connection.cursor() 

        ngrams = NGramManager.generate_ngrams(seed, n=self._n - 1, pad_right=False)

        target_ngrams = NGramManager.generate_ngrams(ending, n=self._n - 1, pad_right=False, left_pad_symbol=None)

        if len(ngrams) == 0:
            seed_ngram = [LEFT_PAD_SYMBOL] * (self._n - 1)
        else:
            seed_ngram = ngrams[-1]

        if len(target_ngrams) == 0:
            target_ngram = [RIGHT_PAD_SYMBOL] * (self._n - 1)
        else:
            target_ngram = target_ngrams[0]

        sql = f"""
        WITH GeneratedSentance AS
        (
            -- Base condition
            SELECT
                TokenText1 || " " || TokenText2 || " " || TokenText3 AS SentanceText, 
                TokenText2,
                TokenText3,
                1 AS Depth
                {',RANDOM()' if rand else ''} 
            FROM
                NGram{self._n}
            WHERE
                (
                    TokenText1 = :seedw1 
                )
                AND
                (
                    TokenText2 = :seedw2
                )

            UNION ALL

            SELECT
                GS.SentanceText || " " || NG.TokenText3 AS SentanceText,
                NG.TokenText2,
                NG.TokenText3,
                GS.Depth + 1
                {',RANDOM()' if rand else ''} 
            FROM
                GeneratedSentance GS
                JOIN NGram{self._n} NG ON
                    NG.TokenText1 = GS.TokenText2 AND
                    NG.TokenText2 = GS.TokenText3
            WHERE
                GS.Depth <= :max_depth
            {'ORDER BY RANDOM()' if rand else ''} 
            LIMIT :row_limit 

        ) 
        SELECT
            GS.SentanceText
        FROM
            GeneratedSentance GS
        WHERE
            GS.Depth >= :min_depth AND
            (
                GS.TokenText2 = :targetw1 
                OR 
                :targetw1 IS NULL
            )
            AND
            (
                GS.TokenText3 = :targetw2
                OR
                :targetw2 IS NULL
            )
        LIMIT
            :limit
        """

        cursor.execute(sql, {  
                "limit": limit,
                "end_token": RIGHT_PAD_SYMBOL, 
                "min_depth": min_length, 
                "max_depth": max_length, 
                "seedw1": seed_ngram[0], 
                "seedw2": seed_ngram[1], 
                "targetw1": target_ngram[0],
                "targetw2": target_ngram[1],
                "row_limit": row_limit
            })

        try:
            return list(map(lambda x: x[0], cursor.fetchall()))
        finally: 
            connection.close()


    def insert(self, text):

        connection = sqlite3.connect(self._db_path)

        cursor = connection.cursor() 

        sql = f"""
            INSERT OR IGNORE INTO NGram{self._n} (
                {",".join(map(lambda i: "TokenText" + str(i), range(1, self._n + 1)))}
            )
            VALUES
            (
                {",".join(["?"] * self._n)}
            )"""
            
        for line in text.split("\n"):

            if line.strip() == "":
                continue
            
            ngrams = NGramManager.generate_ngrams(line, self._n)

            cursor.executemany(sql, ngrams)


        connection.commit()
        connection.close()
        
        

