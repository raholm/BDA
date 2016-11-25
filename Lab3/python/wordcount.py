def word_count(filename):
    lines = sc.textFile(filename)

    words = lines.flatMap(lambda line: line.split(" "))
    words = words.filter(lambda word: len(word) > 0)
    word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda wc1, wc2: wc1 + wc2)
    word_counts = word_counts.sortBy(ascending=False, keyfunc=lambda (key, value): value)

    wordcount = words.count()
    distinct_wordcount = words.distinct().count()

    print(word_counts.collect())
    print(wordcount)
    print(distinct_wordcount)

def main():
    word_count("./test.py")

main()
