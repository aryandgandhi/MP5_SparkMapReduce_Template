#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopTitleStatistics")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1)

for line in lines.take(5):
    print(line)

pairs = lines.map(lambda line: line.split("\t"))

first_five = pairs.take(10)
sums = 0
mean = 0
maxi = float("-inf")
mini = float("inf")
var = 0
count = 0
for i in first_five:
    value = int(i[1])
    
    sums += value
    maxi = max(value, maxi)
    mini = min(value, mini)
    count += 1

cur_mean = int(sums/ count)
stdev = 0 
for i in first_five:
    value = int(i[1])
    stdev += ((value - cur_mean) **2)



ans1 = int(sums/count)
ans2 = int(sums)
ans3 = int(mini)
ans4 = int(maxi)
ans5 = int(stdev/count)

print(ans1,ans2,ans3,ans4,ans5)
    
# total = lines.sum()
# print(total)
#result = line.agg({"columnName": "mean", "columnName": "sum"})

outputFile = open(sys.argv[2], "w")
outputFile.write('Mean\t%s\n' % ans1)
outputFile.write('Sum\t%s\n' % ans2)
outputFile.write('Min\t%s\n' % ans3)
outputFile.write('Max\t%s\n' % ans4)
outputFile.write('Var\t%s\n' % ans5)
'''
TODO write your output here
write results to output file. Format

'''

sc.stop()

