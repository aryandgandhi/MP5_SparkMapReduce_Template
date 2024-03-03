#!/usr/bin/env python

'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''
import sys
from pyspark import SparkConf, SparkContext


stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]
stored = set()
with open(stopWordsPath) as f:
    for word in f.readlines():
        stored.add(word.strip())


with open(delimitersPath) as f:
    delimiters = [line.strip() for line in f]
    
    #print(f.readlines())

conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[3], 1)

def custom_tokenize(line, delimiters):
    for delimiter in delimiters[0]:
        line = line.replace(delimiter, " ")
    return line.split()


#didn't remember what if stop word existed in the repsective word is an issue.

# def remove_stopwords(line):
#     for i in stored:
#         to_remove = i[0: (len(i) - 2)]
#         line = line.replace(to_remove, " ")
#     return line.split()





tokens = lines.flatMap(lambda line: custom_tokenize(line, delimiters))

filtered_tokens = tokens.filter(lambda token: token.lower() not in stored)

wordCounts = filtered_tokens.map(lambda word: (word.lower(), 1)).reduceByKey(lambda a, b: a + b)
collectedWordCounts = wordCounts.collect()

topNWordsByCount = wordCounts.takeOrdered(10, key=lambda x: -x[1])

topNWordsSortedAlphabetically = sorted(topNWordsByCount, key=lambda x: x[0])

with open(sys.argv[4], "w", encoding='utf-8') as outputFile:
    for word, count in topNWordsSortedAlphabetically:
        outputFile.write(f"{word}\t{count}\n")




sc.stop()
