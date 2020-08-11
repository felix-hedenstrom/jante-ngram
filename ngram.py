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
                [f"TokenText{i} TEXT NOT NULL" for i in range(1, self._n + 1)])}
            ,PRIMARY KEY ({",".join(
                            [f"TokenText{i}" for i in range(1, n + 1)])})
        )"""

        cursor = self._connection.cursor()
        cursor.execute(init_sql)
        self._connection.commit()

    def generate_ngrams(text, n, pad_left=True, pad_right=True, left_pad_symbol=LEFT_PAD_SYMBOL, right_pad_symbol=RIGHT_PAD_SYMBOL):
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
            {",".join(
                [f"TokenText{i}" for i in range(1, self._n + 1)])} 
        FROM
            NGram{self._n}
        """

        with self._connection_lock: 
            cursor = self._connection.cursor() 
            cursor.execute(sql)

            return cursor.fetchall()

    def generate(self, seed="", ending="", max_length=None, min_length=2, limit=LimitType.Unlimited, row_limit=10000000, rand=False, strip=True, partial=False):
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
        strip : bool
            should the start and end tokens be stripped?
        partial : bool
            should partial matches be returned? 
            Example:
                
                Insert the sentence "Hello I love dogs because they are cute!" into an empty NGramManager

                using partial=True with seed="I" and ending="dogs" the sentence "I love dogs" could be generated.

                Without it, no results could be found, since there is no ngram [LEFT_PAD_SYMBOL, ..., "I"] in the NGramManager.

                This does not apply to 2Gram, since they don't get padded and only the exact word is targetet.
        """

        if max_length is None:
            max_length = min_length + 10

        if max_length < min_length:
            raise ValueError("max_length cannot be lower than min_length") 

        seed_ngrams = NGramManager.generate_ngrams(seed, n=self._n - 1, pad_left=not partial, pad_right=False)

        target_ngrams = NGramManager.generate_ngrams(ending, n=self._n - 1, pad_left=False, pad_right=not partial)

        if len(seed_ngrams) == 0:
            if partial:
                seed_ngram = []
            else:
                seed_ngram = [LEFT_PAD_SYMBOL] * (self._n - 1)
        else:
            seed_ngram = seed_ngrams[-1]

        if len(target_ngrams) == 0:
            if partial:
                target_ngram = []
            else:
                target_ngram = [RIGHT_PAD_SYMBOL] * (self._n - 1)
        else:
            target_ngram = target_ngrams[0]

        #print(seed, partial, seed_ngram)

        sql = f"""
        WITH GeneratedSentence AS
        (
            -- Base condition
            SELECT
                {' || " " || '.join(
                    [f"TokenText{i}" for i in range(1, self._n + 1)])} AS SentenceText,
                {",".join(
                    [f"TokenText{i}" for i in range(2, self._n + 1)])},
                1 AS Depth
                {',RANDOM()' if rand else ''} 
            FROM
                NGram{self._n}
            WHERE
                {" AND ".join(
                    [f"( TokenText{i} = :seedw{i} )" for i in range(1, self._n)])}

            UNION ALL

            SELECT
                GS.SentenceText || " " || NG.TokenText{self._n} AS SentenceText,
                {",".join(
                    [f"NG.TokenText{i}" for i in range(2, self._n + 1)])},
                GS.Depth + 1
                {',RANDOM()' if rand else ''} 
            FROM
                GeneratedSentence GS
                JOIN NGram{self._n} NG ON
                    {" AND ".join(
                        [f"( NG.TokenText{i} = GS.TokenText{i + 1} )" for i in range(1, self._n)])}
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
                [f" ( GS.TokenText{i + 1} = :targetw{i} OR :targetw{i} IS NULL ) " for i in range(1, self._n)]) }
        {"" if limit == LimitType.Unlimited else "LIMIT :limit"} 
        """


        parameters = {
            "limit": limit,
            "min_depth": min_length, 
            "max_depth": max_length, 
            "row_limit": row_limit
        }

        for i in range(len(seed_ngram)): 
            parameters[f"seedw{i+1}"] = seed_ngram[i]

        for i in range(len(target_ngram)):
            parameters[f"targetw{i+1}"] = target_ngram[i]

        with self._connection_lock:
            cursor = self._connection.cursor() 
            cursor.execute(sql, parameters)
            answers = [x[0] for x in cursor.fetchall()]

        answer_prefix = " ".join([t[0] for t in seed_ngrams[:-1]]) 
        answer_suffix = " ".join([t[0] for t in target_ngrams[self._n - 1:]]) 

        pure_answers = [f"{answer_prefix} {answer} {answer_suffix}" for answer in answers]

        if strip:
            return [re.sub(f"({LEFT_PAD_SYMBOL})|({RIGHT_PAD_SYMBOL})", "", answer).strip() for answer in pure_answers]

        return pure_answers

    def insert(self, text):

        sql = f"""
            INSERT OR IGNORE INTO NGram{self._n} (
                {",".join(
                    [f"TokenText{i}" for i in range(1, self._n + 1)])}
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
