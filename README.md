#Integration
import re
from collections import defaultdict
from pyspark.sql import SparkSession
import time


def map_function(text):
    words = text.lower().split()
    return [(word, 1) for word in words]

def reduce_function(mapped_values):
    counts = defaultdict(int)
    for word, count in mapped_values:
        counts[word] += count
    return counts

def map_reduce(text):
    mapped_values = map_function(text)
    reduced_values = reduce_function(mapped_values)
    return reduced_values


file_path = 'path/to/your/file.txt'
with open(file_path, 'r', encoding='utf-8') as file:
    text_data = file.read()


start_time = time.time()

preprocessed_data = map_reduce(text_data)


# initialize SparkSession

spark = SparkSession.builder.appName("WordCount").getOrCreate()

word_counts_rdd = spark.sparkContext.parallelize(preprocessed_data.items())

# get top 20
top_20_words = word_counts_rdd.takeOrdered(20, key=lambda x: -x[1])
for word, count in top_20_words:
    print(f"{word}: {count}")

# stop SparkSession
spark.stop()

end_time = time.time()

# executed time
execution_time = end_time - start_time
print(f"{execution_time}seconds")

#MapReduce
from collections import Counter, defaultdict
import time


with open('path/to/your/file.txt', 'r', encoding='utf-8') as file:
    text = file.read()


def map_function(text):
    words = text.lower().split()
    return [(word, 1) for word in words]

def reduce_function(mapped_values):
    counts = defaultdict(int)
    for word, count in mapped_values:
        counts[word] += count
    return counts

# process of mapreduce
def map_reduce(text):
    mapped_values = map_function(text)
    reduced_values = reduce_function(mapped_values)
    top_20_words = Counter(reduced_values).most_common(20)
    return top_20_words


start_time = time.time()

top_words = map_reduce(text)

for word, count in top_words:
    print(f"{word}: {count}")

end_time = time.time()

execution_time = end_time - start_time
print(f"{execution_time}seconds")

#Spark
import sys
import time
from pyspark import SparkContext

# Check arguments
if len(sys.argv) != 3:
    print("Usage: termppspark.py <input_file_path> <output_path>", file=sys.stderr)
    exit(-1)

# Initialize SparkContext
sc = SparkContext(appName="Top20WordsExample")

# Start timing
start_time = time.time()

# Input and output paths from command-line arguments
input_file_path = sys.argv[1] 
output_path = sys.argv[2]      

# Read text data and create an RDD
lines = sc.textFile(input_file_path)

# Flatten, map, and reduce to count words
words = lines.flatMap(lambda line: line.split(" ")) \
             .filter(lambda word: word != "") \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)

# Get the top 20 words
top20_words = words.takeOrdered(20, key=lambda x: -x[1])

# Calculate running time
end_time = time.time()
running_time = end_time - start_time

# Convert top 20 words into an RDD (for saving)
top20_rdd = sc.parallelize(top20_words)
