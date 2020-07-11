#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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

    def generate_ngrams(text, n, pad_right=True, pad_left=True, right_pad_symbol=RIGHT_PAD_SYMBOL, left_pad_symbol=LEFT_PAD_SYMBOL):
        return list(nltk.ngrams(
                nltk.pad_sequence(
                    text.split(),
                    pad_left=pad_left, left_pad_symbol=left_pad_symbol,
                    pad_right=pad_right, right_pad_symbol=right_pad_symbol,
                    n=n),
                 n=n))

    def generate(self, seed="", ending="", max_length=None, min_length=4, limit=1, row_limit=10000000):
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

        cursor = self._connection.cursor() 

        ngrams = NGramManager.generate_ngrams(seed, n=2, pad_right=False)

        target_ngrams = NGramManager.generate_ngrams(ending, n=2, pad_right=False, left_pad_symbol=None)

        if len(ngrams) == 0:
            seed_ngram = [LEFT_PAD_SYMBOL] * 3 
        else:
            seed_ngram = ngrams[-1]

        if len(target_ngrams) == 0:
            target_ngram = [RIGHT_PAD_SYMBOL] * 3
        else:
            target_ngram = target_ngrams[0]

        cursor.execute("""
        WITH GeneratedSentance AS
        (
            -- Base condition
            SELECT
                TokenText1 || " " || TokenText2 || " " || TokenText3 AS SentanceText, 
                TokenText2,
                TokenText3,
                1 AS Depth
            FROM
                NGram
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
            FROM
                GeneratedSentance GS
                JOIN NGram NG ON
                    NG.TokenText1 = GS.TokenText2 AND
                    NG.TokenText2 = GS.TokenText3
            WHERE
                GS.Depth <= :max_depth
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
        """, {  
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

        return list(map(lambda x: x[0], cursor.fetchall()))


    def insert(self, text):

        cursor = self._connection.cursor() 
        
            
        for line in text.split("\n"):

            if line.strip() == "":
                continue
            
            ngrams = NGramManager.generate_ngrams(line, 3)

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
        
        

