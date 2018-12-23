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

class AvgLen(MyMRJob):

    def mapper(self, _, line):
        for word in pattern.findall(line):
            yield 1, len(word)

    def reducer(self, _, length):
        n_words = 0
        sum_len = 0
        for l in length:
            n_words += 1
            sum_len += l
        yield None, sum_len / n_words



if __name__ == '__main__':
    
    AvgLen.run()
