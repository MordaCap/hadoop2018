import re
import string
import unicodedata
from mrjob.job import MRJob
from mrjob.protocol import TextProtocol, ReprProtocol

pattern = re.compile(r"[a-zA-Z]+")


def split_with_re(line, re_compiler=pattern):
        translator = str.maketrans(string.punctuation, ' '*len(string.punctuation))
        return re_compiler.findall(line.translate(translator))


class MyMRJob(MRJob):
    OUTPUT_PROTOCOL = ReprProtocol

class MostFreq(MyMRJob):

    def mapper(self, _, line):
        words = [w for w in split_with_re(line, re_compiler=pattern) if w.isalpha()]
        for word in words:
            yield word.lower(), 1
    
    def combiner(self, word, counts):
        yield None, (sum(counts), word)
    
    def reducer(self, _, words):
        dic = {}
        for count, word in words:
            dic[word] = dic.setdefault(word, 1) + count
        yield max(dic.items(), key=lambda x:x[1])

if __name__ == '__main__':
   
    MostFreq.run()
