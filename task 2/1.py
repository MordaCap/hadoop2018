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

class MaxLengthWord(MyMRJob):
    
    def mapper(self, _, line):
        for word in pattern.findall(line):
            yield None, (len(word), word)

    def reducer(self, _, word):
        yield max(word)

if __name__ == '__main__':

    MaxLengthWord.run()
