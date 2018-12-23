import re
import string
import unicodedata
from mrjob.job import MRJob
from mrjob.protocol import TextProtocol, ReprProtocol


pattern = re.compile(r"[\w']+")


def split_with_re(line, re_compiler=pattern):
        translator = str.maketrans(string.punctuation, ' '*len(string.punctuation))
        return re_compiler.findall(line.translate(translator))


class MyMRJob(MRJob):
    OUTPUT_PROTOCOL = ReprProtocol


class TargetWords(MyMRJob):

    def mapper(self, _, line):
        words = [w for w in split_with_re(line)]
        for word in words:
            yield word.lower(), int(word[0].isupper())
    
    def combiner(self, word, counts):
        n_words = 0
        n_capital = 0
        for c in counts:
            n_words += 1
            n_capital = c
        yield None, (n_words, n_capital, word)
    
    def reducer(self, _, words):
        dic = {}
        for n_words, n_capital, word in words:
            dic[word] = dic.setdefault(word, [0, 0])
            dic[word][0] += n_words
            dic[word][1] += n_capital
        for word in dic:
            if dic[word][0] > 10 and 2 * dic[word][1] > dic[word][0]:
                yield word, dic[word]



if __name__ == '__main__':

    TargetWords.run()
