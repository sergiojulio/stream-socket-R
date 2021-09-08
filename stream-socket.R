# load library
library(SparkR)

sparkR.session(appName = "StructuredNetworkWordCount")

# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines <- read.stream("socket", host = "localhost", port = 9999)

# Split the lines into words
words <- selectExpr(lines, "explode(split(value, ' ')) as word")

# Generate running word count
wordCounts <- count(group_by(words, "word"))

# Start running the query that prints the running counts to the console
query <- write.stream(wordCounts, "console", outputMode = "complete")

# Start running the query that prints the running counts to the console
query <- write.stream(wordCounts, "console", outputMode = "complete")

awaitTermination(query)

