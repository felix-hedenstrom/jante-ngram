#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sqlite3
import nltk
import os
import re

from enum import Enum
from threading import Lock

LEFT_PAD_SYMBOL = "<s>"
RIGHT_PAD_SYMBOL = "</s>"
NEWLINE = os.linesep 

class LimitType:
    Unlimited = 1

class NGramManager:

    def __init__(self, path, n=3):

        if not type(n) == int:
            return ValueError("n must be an integer")

        self._n = n
        self._db_path = path
        self._connection_lock = Lock()
        self._connection = sqlite3.connect(self._db_path, check_same_thread=False)

        init_sql = f"""
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

        cursor = self._connection.cursor()
        cursor.execute(init_sql)
        self._connection.commit()

    def generate_ngrams(text, n, pad_right=True, pad_left=True, right_pad_symbol=RIGHT_PAD_SYMBOL, left_pad_symbol=LEFT_PAD_SYMBOL):
        return list(nltk.ngrams(
                nltk.pad_sequence(
                    text.split(),
                    pad_left=pad_left, left_pad_symbol=left_pad_symbol,
                    pad_right=pad_right, right_pad_symbol=right_pad_symbol,
                    n=n),
                 n=n))
    
    def get_all(self):

        sql = f"""
        SELECT
            {",".join(map(lambda i: "TokenText" + str(i), range(1, self._n + 1)))} 
        FROM
            NGram{self._n}
        """

        with self._connection_lock: 
            cursor = self._connection.cursor() 
            cursor.execute(sql)

            return cursor.fetchall()

    def generate(self, seed="", ending="", max_length=None, min_length=2, limit=LimitType.Unlimited, row_limit=10000000, rand=False, strip=True):
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


        seed_ngrams = NGramManager.generate_ngrams(seed, n=self._n - 1, pad_right=False)

        target_ngrams = NGramManager.generate_ngrams(ending, n=self._n - 1, pad_left=False)

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
        {"LIMIT :limit" if limit != LimitType.Unlimited else ""} 
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

        with self._connection_lock:
            cursor = self._connection.cursor() 
            cursor.execute(sql, parameters)
            answers = map(lambda x: x[0], cursor.fetchall())

        answer_prefix = " ".join(map(lambda t: t[0], seed_ngrams[:-1])) 
        answer_suffix = " ".join(map(lambda t: t[0], target_ngrams[self._n - 1:])) 

        pure_answers = list(map(lambda answer: f"{answer_prefix} {answer} {answer_suffix}", answers)) 

        if strip:
            return [re.sub(f"({LEFT_PAD_SYMBOL})|({RIGHT_PAD_SYMBOL})", "", answer).strip() for answer in pure_answers]

        return pure_answers

    def insert(self, text):

        sql = f"""
            INSERT OR IGNORE INTO NGram{self._n} (
                {",".join(map(lambda i: "TokenText" + str(i), range(1, self._n + 1)))}
            )
            VALUES
            (
                {",".join(["?"] * self._n)}
            )"""

        with self._connection_lock:
            cursor = self._connection.cursor() 
                
            for line in text.split("\n"):

                if line.strip() == "":
                    continue
                
                ngrams = NGramManager.generate_ngrams(line, self._n)

                cursor.executemany(sql, ngrams)


            self._connection.commit()

    def __del__(self):
        with self._connection_lock:
            self._connection.close()
