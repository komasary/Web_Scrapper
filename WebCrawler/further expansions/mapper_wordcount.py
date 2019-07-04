#!/usr/bin/env python
"""mapper.py"""

import sys
import re
from nltk.stem.snowball import SnowballStemmer

stemmer = SnowballStemmer("english")

stopwords = ["cycle"]
# input comes from STDIN (standard input)
for line in sys.stdin:
    sublines = line.split(",")
    #Words are more like an array of the words now

    for subline in sublines:
        words = subline.split()

        for word in words:
            word = word.lower()
            # write the results to STDOUT (standard output);
            # what we output here will be the input for the
            # Reduce step, i.e. the input for reducer.py
            if word in stopwords:
                continue
            else:
                word = re.sub(r'\W+', '', word)
                alphamatch = re.search('[a-zA-Z]', word)
                if alphamatch:
                    print('%s\t%s' % (stemmer.stem(word), 1))