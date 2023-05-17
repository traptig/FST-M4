-- Load input file from HDFS
inputFile = LOAD 'hdfs:///user/root/episodeV_dialouges.txt' AS (line);
-- Tokeize each word in the file (Map)
words = FOREACH inputFile GENERATE FLATTEN(TOKENIZE(line)) AS word;
-- Combine the words from the above stage
grpd = GROUP words BY word;
-- Count the occurence of each word (Reduce)
word_count = FOREACH grpd GENERATE group AS word, COUNT(words) AS count;
ordered_word_count = ORDER word_count BY count DESC;
-- Store the result in HDFS
STORE ordered_word_count INTO 'hdfs:///user/root/episodeVoutput';