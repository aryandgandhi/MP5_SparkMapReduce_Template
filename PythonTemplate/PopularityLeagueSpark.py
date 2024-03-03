#!/usr/bin/env python

#Execution Command: spark-submit PopularityLeagueSpark.py dataset/links/ dataset/league.txt
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularityLeague")
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

leagueIds = sc.textFile(sys.argv[2], 1).map(lambda x: int(x))

leagueIdsBroadcast = sc.broadcast(set(leagueIds.collect()))

filtered_linked_pages = sorted_linked_pages.filter(lambda x: x[0] in leagueIdsBroadcast.value)

filtered_results = filtered_linked_pages.collect()

filt = sorted(filtered_results, key = lambda x: x[1])
 

cur_count = 0
total_count = 0
first = True
prev_val = filtered_results[0][1]
ret = []
for key,val in filt:

    if prev_val == val:
        cur_count += 0
        total_count += 1
    if prev_val != val and not first:
        cur_count += total_count
        cur_count += 1
        total_count = 0
    prev_val = val
    first = False
    
    ret.append((str(key),cur_count))

ret.sort()




with open(sys.argv[3], "w") as output:
    for key, value in ret:
        output.write(f"{key}\t{value}\n")

#write results to output file. Foramt for each line: (key + \t + value +"\n")

sc.stop()

