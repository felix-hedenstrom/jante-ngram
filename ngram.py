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
        """Generate sentences from the ngrams

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

        seed_ngrams = NGramManager.generate_ngrams(seed, n=self._n - 1, pad_right=False)


        target_ngrams = NGramManager.generate_ngrams(ending, n=self._n - 1, pad_right=False, left_pad_symbol=None)

        if len(seed_ngrams) == 0:
            seed_ngram = [LEFT_PAD_SYMBOL] * (self._n - 1)
        else:
            seed_ngram = seed_ngrams[-1]

        if len(target_ngrams) == 0:
            target_ngram = [RIGHT_PAD_SYMBOL] * (self._n - 1)
        else:
            target_ngram = target_ngrams[0]

        sql = f"""
        WITH GeneratedSentence AS
        (
            -- Base condition
            SELECT
                {' || " " || '.join(
                    map(
                        lambda i: f"TokenText{i}", 
                        range(1, self._n + 1)))} AS SentenceText,
                {",".join(
                    map(
                        lambda i: f"TokenText{i}", 
                        range(2, self._n + 1)))},
                1 AS Depth
                {',RANDOM()' if rand else ''} 
            FROM
                NGram{self._n}
            WHERE
                {" AND ".join(
                    map(
                        lambda i: f"( TokenText{i} = :seedw{i} )", 
                        range(1, self._n)))}

            UNION ALL

            SELECT
                GS.SentenceText || " " || NG.TokenText{self._n} AS SentenceText,
                {",".join(
                    map(
                        lambda i: f"NG.TokenText{i}", 
                        range(2, self._n + 1)))},
                GS.Depth + 1
                {',RANDOM()' if rand else ''} 
            FROM
                GeneratedSentence GS
                JOIN NGram{self._n} NG ON
                    {" AND ".join(
                        map(
                            lambda i: f"( NG.TokenText{i} = GS.TokenText{i + 1} )", 
                            range(1, self._n)))}
            WHERE
                GS.Depth <= :max_depth
            {'ORDER BY RANDOM()' if rand else ''} 
            LIMIT :row_limit 

        ) 
        SELECT
            GS.SentenceText
        FROM
            GeneratedSentence GS
        WHERE
            GS.Depth >= :min_depth AND
            { " AND ".join(
                map(
                    lambda i: f" ( GS.TokenText{i + 1} = :targetw{i} OR :targetw{i} IS NULL ) ", 
                    range(1, self._n))) }
        LIMIT
            :limit
        """


        parameters = {
            "limit": limit,
            "min_depth": min_length, 
            "max_depth": max_length, 
            "row_limit": row_limit
        }

        for i in range(0, self._n - 1):
            parameters[f"seedw{i+1}"] = seed_ngram[i]
            parameters[f"targetw{i+1}"] = target_ngram[i]

        #print(sql)
        print(seed_ngram)
        #print(target_ngram)
        #print(parameters)

        cursor.execute(sql, parameters)
        answers = map(lambda x: x[0], cursor.fetchall())
        connection.close()

        answer_prefix = " ".join(map(lambda t: t[0], seed_ngrams[:-1])) 

        return list(map(lambda answer: f"{answer_prefix} {answer}", answers)) 


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
