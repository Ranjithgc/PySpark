"""
@Author: Ranjith G C
@Date: 2021-08-16
@Last Modified by: Ranjitth G C
@Last Modified time: 2021-08-16
@Title : Program Aim is to do word count program in pyspark.
"""

import sys
 
from pyspark import SparkContext, SparkConf
 
if __name__ == "__main__":
	
	# create Spark context with necessary configuration
	sc = SparkContext("local","PySpark Word Count Exmaple")
	
	# read data from text file and split each line into words
	words = sc.textFile("/home/ubunta/Desktop/PySpark/Word_count/input.txt").flatMap(lambda line: line.split(" "))
	
	# count the occurrence of each word
	wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
	
	# save the counts to output
	wordCounts.saveAsTextFile("/home/ubunta/Desktop/PySpark/Word_count/output/")