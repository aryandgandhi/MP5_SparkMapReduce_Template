#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("OrphanPages")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 

#outputs format of page_id to link, or multipel links


def parseLine(line):
    parts = line.split(':')
    page_id = int(parts[0])
    links = [int(link) for link in parts[1].split(' ') if link.strip().isdigit()]

    return (page_id, links)


links = lines.map(parseLine)

linked_pages = links.flatMap(lambda x: [(link, 1) for link in x[1]])

all_pages = links.map(lambda x: (x[0], 1))

orphan_pages = all_pages.subtractByKey(linked_pages)

orphan_page_ids = orphan_pages.keys().collect()

output = open(sys.argv[2], "w")

sorted_orphan_page_ids = sorted([str(page_id) for page_id in orphan_page_ids])


with open(sys.argv[2], "w") as output:
    for page_id in sorted_orphan_page_ids:
        output.write(str(page_id) + "\n")


sc.stop()

