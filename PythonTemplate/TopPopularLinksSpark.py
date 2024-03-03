#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopPopularLinks")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 

def parseLine(line):
    parts = line.split(':')
    page_id = int(parts[0])
    links = [int(link) for link in parts[1].split(' ') if link.strip().isdigit()]

    return (page_id, links)

links = lines.map(parseLine)

linked_pages = links.flatMap(lambda x: [(link, 1) for link in x[1]])

sum_linked_pages = linked_pages.reduceByKey(lambda a, b: a + b)

sorted_linked_pages = sum_linked_pages.sortBy(lambda x: x[1], ascending=False)

result = sorted_linked_pages.take(10)

top_5 = [(str(key), value) for key, value in result]

sorted_bottom5_str_keys = sorted(top_5, key=lambda x: x[0])

with open(sys.argv[2], "w", encoding='utf-8') as outputFile:
    for word, count in sorted_bottom5_str_keys:
        outputFile.write(f"{word}\t{count}\n")


sc.stop()

